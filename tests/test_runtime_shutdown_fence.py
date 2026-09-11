from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import threading
from types import SimpleNamespace

import pytest
from sqlalchemy import event, select, text

from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.main import (
    _pause_runtime_for_shutdown,
    _shutdown_runtime_coordinators,
    _stop_runtime_coordinators,
)
from app.models.runtime import RuntimeControlEvent, RuntimeControlState
from app.services.runtime_controls import (
    ControlCommand,
    enable_kill_switch,
    ensure_runtime_control_state,
    resume_runtime,
)
from app.services.alpaca_crypto_runtime import AlpacaCryptoRuntimeCoordinator
from app.services.alpaca_us_equity_runtime import AlpacaUsEquityRuntimeCoordinator
from app.services.autonomous_runtime import AutonomousRuntimeCoordinator
from app.services.runtime_shutdown import (
    CommitGuardedSessionFactory,
    GuardedBrokerAdapter,
    RuntimeCycleFence,
)
from app.services.position_identity import EXECUTION_SCOPE_POLICY_VERSION


def test_lifespan_shutdown_durably_pauses_automation_with_audit_source(
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'shutdown-pause.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            ensure_runtime_control_state(settings=settings, db=db)
            resume_runtime(
                settings=settings,
                db=db,
                command=ControlCommand(source="test", reason="prepare running state"),
            )

        asyncio.run(
            _pause_runtime_for_shutdown(
                settings=settings,
                session_factory=session_factory,
            )
        )

        with session_factory() as db:
            state = db.get(RuntimeControlState, "global")
            assert state is not None
            assert state.paused is True
            assert state.automation_enabled is False
            assert state.last_transition_source == "lifespan_shutdown"
            event = db.scalar(
                select(RuntimeControlEvent)
                .where(RuntimeControlEvent.action == "pause")
                .order_by(RuntimeControlEvent.occurred_at.desc())
            )
            assert event is not None
            assert event.source == "lifespan_shutdown"
    finally:
        engine.dispose()


@pytest.mark.parametrize("initial_state", ["paused", "kill_switch"])
def test_lifespan_shutdown_never_implicitly_reenables_a_blocked_runtime(
    initial_state: str,
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / f'shutdown-{initial_state}.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as db:
            state = ensure_runtime_control_state(settings=settings, db=db)
            if initial_state == "kill_switch":
                result = enable_kill_switch(
                    settings=settings,
                    db=db,
                    command=ControlCommand(
                        source="operator_test",
                        reason="prepare kill-switch state",
                    ),
                )
                assert result.outcome == "applied"
                state = db.get(RuntimeControlState, "global")
                assert state is not None
            prior_transition_source = state.last_transition_source

        asyncio.run(
            _pause_runtime_for_shutdown(
                settings=settings,
                session_factory=session_factory,
            )
        )

        with session_factory() as db:
            state = db.get(RuntimeControlState, "global")
            assert state is not None
            assert state.paused is True
            assert state.automation_enabled is False
            assert state.kill_switch_enabled is (initial_state == "kill_switch")
            assert state.last_transition_source == prior_transition_source
            event = db.scalar(
                select(RuntimeControlEvent)
                .where(RuntimeControlEvent.action == "pause")
                .order_by(RuntimeControlEvent.occurred_at.desc())
            )
            assert event is not None
            assert event.outcome == "noop"
            assert event.source == "lifespan_shutdown"
    finally:
        engine.dispose()


def test_lifespan_shutdown_pauses_before_stopping_any_coordinator(tmp_path) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'shutdown-order.db'}",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    calls: list[str] = []

    class Coordinator:
        def __init__(self, name: str) -> None:
            self.name = name

        async def stop(self) -> None:
            with session_factory() as db:
                state = db.get(RuntimeControlState, "global")
                assert state is not None
                assert state.paused is True
            calls.append(self.name)

    try:
        with session_factory() as db:
            ensure_runtime_control_state(settings=settings, db=db)
            resume_runtime(
                settings=settings,
                db=db,
                command=ControlCommand(source="test", reason="prepare running state"),
            )

        asyncio.run(
            _shutdown_runtime_coordinators(
                settings=settings,
                session_factory=session_factory,
                coordinators=(
                    ("one", Coordinator("one")),
                    ("two", Coordinator("two")),
                ),
            )
        )

        assert calls == ["one", "two"]
    finally:
        engine.dispose()


def test_in_memory_shutdown_drains_shared_connection_before_durable_pause() -> None:
    settings = Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="paper",
    )
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    worker_started = threading.Event()
    release_worker = threading.Event()
    worker_stopped = threading.Event()
    pause_insert_seen = threading.Event()
    worker_errors: list[BaseException] = []
    calls: list[str] = []

    with session_factory() as db:
        ensure_runtime_control_state(settings=settings, db=db)
        resume_runtime(
            settings=settings,
            db=db,
            command=ControlCommand(source="test", reason="prepare running state"),
        )

    def worker_transaction() -> None:
        try:
            with session_factory() as db:
                db.execute(select(RuntimeControlState))
                worker_started.set()
                assert release_worker.wait(timeout=2)
                db.rollback()
        except BaseException as exc:  # pragma: no cover - assertion reports detail
            worker_errors.append(exc)
        finally:
            worker_stopped.set()

    worker_thread = threading.Thread(target=worker_transaction)
    worker_thread.start()
    assert worker_started.wait(timeout=1)

    def interrupt_pause_insert(
        _connection,
        _cursor,
        statement: str,
        _parameters,
        _context,
        _executemany: bool,
    ) -> None:
        if "INSERT INTO runtime_control_events" not in statement:
            return
        calls.append("pause_inserted")
        pause_insert_seen.set()
        release_worker.set()
        assert worker_stopped.wait(timeout=2)

    event.listen(engine, "after_cursor_execute", interrupt_pause_insert)

    class Coordinator:
        def begin_shutdown(self) -> None:
            calls.append("admission_closed")

        async def stop(self) -> None:
            calls.append("coordinator_stopped")
            release_worker.set()
            stopped = await asyncio.to_thread(worker_stopped.wait, 2)
            assert stopped

    try:
        asyncio.run(
            _shutdown_runtime_coordinators(
                settings=settings,
                session_factory=session_factory,
                coordinators=(("runtime", Coordinator()),),
            )
        )

        assert calls == [
            "admission_closed",
            "coordinator_stopped",
            "pause_inserted",
        ]
        assert pause_insert_seen.is_set() is True
        assert worker_errors == []
        with session_factory() as db:
            state = db.get(RuntimeControlState, "global")
            assert state is not None
            assert state.paused is True
            pause_event = db.scalar(
                select(RuntimeControlEvent)
                .where(
                    RuntimeControlEvent.action == "pause",
                    RuntimeControlEvent.source == "lifespan_shutdown",
                )
                .order_by(RuntimeControlEvent.occurred_at.desc())
            )
            assert pause_event is not None
    finally:
        event.remove(engine, "after_cursor_execute", interrupt_pause_insert)
        release_worker.set()
        worker_thread.join(timeout=2)
        engine.dispose()


def test_lifespan_closes_broker_admission_before_durable_pause_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        pause_started = asyncio.Event()
        release_pause = asyncio.Event()
        fence = RuntimeCycleFence()
        broker_calls: list[str] = []

        async def delayed_pause(**_kwargs: object) -> None:
            pause_started.set()
            await release_pause.wait()

        class Coordinator:
            def begin_shutdown(self) -> None:
                fence.begin_shutdown()

            async def stop(self) -> None:
                return None

        class Adapter:
            def place_order(self, *_args: object, **_kwargs: object) -> object:
                broker_calls.append("placed")
                return object()

        monkeypatch.setattr("app.main._pause_runtime_for_shutdown", delayed_pause)
        shutdown_task = asyncio.create_task(
            _shutdown_runtime_coordinators(
                settings=Settings(database_url="sqlite:///:memory:"),
                session_factory=None,  # type: ignore[arg-type]
                coordinators=(("runtime", Coordinator()),),
            )
        )
        await asyncio.wait_for(pause_started.wait(), timeout=1)

        with pytest.raises(asyncio.CancelledError):
            GuardedBrokerAdapter(Adapter(), fence).place_order("token", object())
        assert broker_calls == []

        release_pause.set()
        await asyncio.wait_for(shutdown_task, timeout=1)

    asyncio.run(scenario())


def test_lifespan_waits_for_real_drain_after_bounded_stop_timeout() -> None:
    async def scenario() -> None:
        release_worker = asyncio.Event()
        drain_started = asyncio.Event()
        calls: list[str] = []

        class TimedOutCoordinator:
            async def stop(self) -> None:
                calls.append("stop:timed_out")
                raise TimeoutError("worker did not drain")

            async def wait_stopped(self) -> None:
                calls.append("wait:timed_out")
                drain_started.set()
                await release_worker.wait()

        class FastCoordinator:
            async def stop(self) -> None:
                calls.append("stop:fast")

        shutdown_task = asyncio.create_task(
            _stop_runtime_coordinators(
                (
                    ("timed_out", TimedOutCoordinator()),
                    ("fast", FastCoordinator()),
                )
            )
        )
        await asyncio.wait_for(drain_started.wait(), timeout=1)

        assert shutdown_task.done() is False
        assert calls == ["stop:timed_out", "stop:fast", "wait:timed_out"]

        release_worker.set()
        with pytest.raises(ExceptionGroup) as exc_info:
            await asyncio.wait_for(shutdown_task, timeout=1)

        assert len(exc_info.value.exceptions) == 1
        assert isinstance(exc_info.value.exceptions[0], TimeoutError)

    asyncio.run(scenario())


def test_cancelled_lifespan_finishes_pause_before_coordinator_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        pause_started = asyncio.Event()
        release_pause = asyncio.Event()
        calls: list[str] = []

        async def delayed_pause(**_kwargs: object) -> None:
            calls.append("pause:start")
            pause_started.set()
            await release_pause.wait()
            calls.append("pause:done")

        class Coordinator:
            async def stop(self) -> None:
                calls.append("stop")

        monkeypatch.setattr("app.main._pause_runtime_for_shutdown", delayed_pause)
        shutdown_task = asyncio.create_task(
            _shutdown_runtime_coordinators(
                settings=Settings(database_url="sqlite:///:memory:"),
                session_factory=None,  # type: ignore[arg-type]
                coordinators=(("runtime", Coordinator()),),
            )
        )
        await asyncio.wait_for(pause_started.wait(), timeout=1)
        shutdown_task.cancel()
        await asyncio.sleep(0.02)

        assert calls == ["pause:start"]

        release_pause.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(shutdown_task, timeout=1)
        assert calls == ["pause:start", "pause:done", "stop"]

    asyncio.run(scenario())


@pytest.mark.parametrize("runtime_kind", ["crypto", "us_equity", "autonomous"])
def test_stop_timeout_keeps_runtime_fenced_until_real_worker_completion(
    runtime_kind: str,
) -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            alpaca_crypto_monitoring_enabled=runtime_kind == "crypto",
            alpaca_us_equity_monitoring_enabled=runtime_kind == "us_equity",
            autonomous_runtime_enabled=runtime_kind == "autonomous",
            nxt_eligible_sync_enabled=False,
        )
        closed: list[str] = []

        class PriceSource:
            def close(self) -> None:
                closed.append("closed")

        if runtime_kind == "crypto":
            runtime = AlpacaCryptoRuntimeCoordinator(
                settings=settings,
                session_factory=None,  # type: ignore[arg-type]
            )
        elif runtime_kind == "us_equity":
            runtime = AlpacaUsEquityRuntimeCoordinator(
                settings=settings,
                session_factory=None,  # type: ignore[arg-type]
            )
        else:
            runtime = AutonomousRuntimeCoordinator(
                settings=settings,
                session_factory=None,  # type: ignore[arg-type]
                broker_adapter_factory=(
                    lambda _settings: None  # type: ignore[arg-type,return-value]
                ),
                monitoring_price_source=PriceSource(),
            )

        entered = threading.Event()
        release = threading.Event()

        def blocking_cycle(*_args: object) -> dict[str, object]:
            entered.set()
            assert release.wait(timeout=2)
            return {"status": "healthy", "status_code": 200}

        runtime._shutdown_drain_timeout_seconds = lambda: 0.02  # type: ignore[method-assign]
        if runtime_kind == "autonomous":
            runtime._dispatch_cycle_sync = blocking_cycle  # type: ignore[method-assign]
            runtime._dispatch_task = asyncio.create_task(
                runtime._run_dispatch_cycle(datetime.now(UTC))
            )
        else:
            runtime.run_cycle = blocking_cycle  # type: ignore[method-assign]
            runtime._task = asyncio.create_task(runtime._run_cycle_in_thread())

        try:
            while not entered.is_set():
                await asyncio.sleep(0.001)
            with pytest.raises(TimeoutError, match="in-flight cycle"):
                await runtime.stop()

            assert runtime._stopping is True
            assert closed == []

            release.set()
            await asyncio.wait_for(runtime.wait_stopped(), timeout=1)

            assert runtime._stopping is False
            assert not runtime._inflight_cycles
            if runtime_kind == "autonomous":
                assert closed == ["closed"]
        finally:
            release.set()
            wait_stopped = getattr(runtime, "wait_stopped", None)
            if callable(wait_stopped):
                await wait_stopped()

    asyncio.run(scenario())


def test_shutdown_generation_blocks_broker_mutation_and_terminal_commit(
    tmp_path,
) -> None:
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'cycle-fence.db'}")
    engine = create_db_engine(settings)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE shutdown_sentinel (value INTEGER)"))
    session_factory = create_session_factory(engine)
    fence = RuntimeCycleFence()
    generation = fence.capture_generation()
    broker_calls: list[str] = []

    class Adapter:
        provider = "test"

        def place_order(self, *_args: object, **_kwargs: object) -> object:
            broker_calls.append("place_order")
            return object()

    adapter = GuardedBrokerAdapter(Adapter(), fence)
    guarded_sessions = CommitGuardedSessionFactory(
        session_factory,
        fence,
    )

    assert isinstance(adapter, Adapter)

    fence.begin_shutdown()

    with pytest.raises(asyncio.CancelledError):
        fence.checkpoint(generation)
    with pytest.raises(asyncio.CancelledError):
        adapter.place_order("token", object())
    with guarded_sessions() as db:
        db.execute(text("INSERT INTO shutdown_sentinel (value) VALUES (1)"))
        with pytest.raises(asyncio.CancelledError):
            db.commit()
        db.rollback()

    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM shutdown_sentinel")) == 0
    assert broker_calls == []
    engine.dispose()


def test_shutdown_preserves_broker_call_that_was_already_admitted() -> None:
    fence = RuntimeCycleFence()
    broker_entered = threading.Event()
    release_broker = threading.Event()
    shutdown_finished = threading.Event()
    calls: list[str] = []

    class Adapter:
        def place_order(self, *_args: object, **_kwargs: object) -> object:
            broker_entered.set()
            assert release_broker.wait(timeout=2)
            calls.append("placed")
            return object()

    adapter = GuardedBrokerAdapter(Adapter(), fence)
    broker_thread = threading.Thread(target=lambda: adapter.place_order("token", object()))
    broker_thread.start()
    assert broker_entered.wait(timeout=1)

    shutdown_thread = threading.Thread(
        target=lambda: (fence.begin_shutdown(), shutdown_finished.set())
    )
    shutdown_thread.start()

    assert shutdown_finished.wait(timeout=1) is True
    release_broker.set()
    broker_thread.join(timeout=1)
    shutdown_thread.join(timeout=1)

    assert calls == ["placed"]
    with pytest.raises(asyncio.CancelledError):
        adapter.place_order("token", object())


def test_shutdown_does_not_cancel_commit_that_already_crossed_the_fence(tmp_path) -> None:
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'commit-linearization.db'}")
    engine = create_db_engine(settings)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE shutdown_sentinel (value INTEGER)"))
    session_factory = create_session_factory(engine)
    fence = RuntimeCycleFence()
    guarded_sessions = CommitGuardedSessionFactory(session_factory, fence)
    commit_entered = threading.Event()
    release_commit = threading.Event()
    shutdown_finished = threading.Event()
    failures: list[BaseException] = []
    db = guarded_sessions()

    def block_after_guard(_session) -> None:  # noqa: ANN001
        commit_entered.set()
        assert release_commit.wait(timeout=2)

    event.listen(db, "before_commit", block_after_guard)
    db.execute(text("INSERT INTO shutdown_sentinel (value) VALUES (1)"))

    def commit() -> None:
        try:
            db.commit()
        except BaseException as exc:  # pragma: no cover - assertion reports detail
            failures.append(exc)

    commit_thread = threading.Thread(target=commit)
    commit_thread.start()
    assert commit_entered.wait(timeout=1)
    shutdown_thread = threading.Thread(
        target=lambda: (fence.begin_shutdown(), shutdown_finished.set())
    )
    shutdown_thread.start()

    assert shutdown_finished.wait(timeout=1) is True
    release_commit.set()
    commit_thread.join(timeout=1)
    shutdown_thread.join(timeout=1)

    assert failures == []
    assert shutdown_finished.is_set() is True
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM shutdown_sentinel")) == 1
    db.close()
    engine.dispose()


def test_commit_admission_never_delays_closing_new_broker_admission() -> None:
    fence = RuntimeCycleFence()
    fence.begin_commit()
    shutdown_finished = threading.Event()
    shutdown_thread = threading.Thread(
        target=lambda: (fence.begin_shutdown(), shutdown_finished.set())
    )
    try:
        shutdown_thread.start()

        assert shutdown_finished.wait(timeout=1) is True
        with pytest.raises(asyncio.CancelledError):
            GuardedBrokerAdapter(SimpleNamespace(place_order=lambda: object()), fence).place_order()
    finally:
        fence.finish_commit()
        shutdown_thread.join(timeout=1)


def test_shutdown_blocks_second_broker_call_in_an_already_admitted_cycle() -> None:
    fence = RuntimeCycleFence()
    generation = fence.capture_generation()
    first_completed = threading.Event()
    attempt_second = threading.Event()
    cycle_finished = threading.Event()
    calls: list[str] = []
    errors: list[BaseException] = []

    class Adapter:
        def place_order(self, label: str) -> object:
            calls.append(label)
            return object()

    adapter = GuardedBrokerAdapter(Adapter(), fence)

    def cycle() -> None:
        try:
            adapter.place_order("first")
            first_completed.set()
            assert attempt_second.wait(timeout=2)
            adapter.place_order("second")
        except BaseException as exc:
            errors.append(exc)
        finally:
            cycle_finished.set()

    cycle_thread = threading.Thread(
        target=lambda: fence.run(generation, cycle)
    )
    cycle_thread.start()
    try:
        assert first_completed.wait(timeout=1)
        fence.begin_shutdown()
        attempt_second.set()
        assert cycle_finished.wait(timeout=1)

        assert calls == ["first"]
        assert len(errors) == 1
        assert isinstance(errors[0], asyncio.CancelledError)
    finally:
        attempt_second.set()
        cycle_thread.join(timeout=1)


def test_admitted_broker_call_persists_ack_but_cannot_admit_another_call(
    tmp_path,
) -> None:
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'broker-ack-fence.db'}")
    engine = create_db_engine(settings)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE broker_ack (value INTEGER)"))
    session_factory = create_session_factory(engine)
    fence = RuntimeCycleFence()
    guarded_sessions = CommitGuardedSessionFactory(session_factory, fence)
    generation = fence.capture_generation()
    broker_entered = threading.Event()
    release_broker = threading.Event()
    calls: list[str] = []
    errors: list[BaseException] = []

    class Adapter:
        def place_order(self, label: str) -> object:
            calls.append(label)
            if label == "first":
                broker_entered.set()
                assert release_broker.wait(timeout=2)
            return object()

    adapter = GuardedBrokerAdapter(Adapter(), fence)

    def cycle() -> None:
        adapter.place_order("first")
        with guarded_sessions() as db:
            db.execute(text("INSERT INTO broker_ack (value) VALUES (1)"))
            db.commit()
        adapter.place_order("second")

    def run_cycle() -> None:
        try:
            fence.run(generation, cycle)
        except BaseException as exc:
            errors.append(exc)

    cycle_thread = threading.Thread(target=run_cycle)
    cycle_thread.start()
    try:
        assert broker_entered.wait(timeout=1)
        fence.begin_shutdown()
        release_broker.set()
        cycle_thread.join(timeout=1)

        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM broker_ack")) == 1
        assert calls == ["first"]
        assert len(errors) == 1
        assert isinstance(errors[0], asyncio.CancelledError)
    finally:
        release_broker.set()
        cycle_thread.join(timeout=1)
        engine.dispose()


def test_nested_transaction_commit_does_not_leak_runtime_fence_lease(
    tmp_path,
) -> None:
    settings = Settings(database_url=f"sqlite:///{tmp_path / 'nested-commit-lease.db'}")
    engine = create_db_engine(settings)
    session_factory = create_session_factory(engine)

    class CountingFence:
        def __init__(self) -> None:
            self.begins = 0
            self.finishes = 0

        def begin_commit(self) -> None:
            self.begins += 1

        def finish_commit(self) -> None:
            self.finishes += 1

    fence = CountingFence()
    guarded_sessions = CommitGuardedSessionFactory(
        session_factory,
        fence,  # type: ignore[arg-type]
    )
    try:
        with guarded_sessions() as db:
            db.execute(text("SELECT 1"))
            with db.begin_nested():
                db.execute(text("SELECT 1"))
            db.commit()

        assert fence.begins == 1
        assert fence.finishes == 1
    finally:
        engine.dispose()


@pytest.mark.parametrize("runtime_kind", ["crypto", "us_equity"])
def test_alpaca_cycle_released_after_stop_timeout_cannot_mutate(
    runtime_kind: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url=f"sqlite:///{tmp_path / f'{runtime_kind}-release.db'}",
            runtime_mode="paper",
            alpaca_crypto_monitoring_enabled=runtime_kind == "crypto",
            alpaca_us_equity_monitoring_enabled=runtime_kind == "us_equity",
            alpaca_crypto_allowed_symbols="BTC/USD",
            alpaca_us_equity_allowed_symbols="SPY",
        )
        engine = create_db_engine(settings)
        Base.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE shutdown_sentinel (value INTEGER)"))
        session_factory = create_session_factory(engine)
        entered = threading.Event()
        release = threading.Event()
        broker_boundary_blocked = threading.Event()
        commit_attempted = threading.Event()
        commit_completed = threading.Event()
        broker_calls: list[str] = []

        class Adapter:
            provider = "alpaca"

            def get_live_market_context(self, *_args: object, **_kwargs: object) -> None:
                return None

            def get_market_clock(self, *_args: object, **_kwargs: object) -> dict[str, object]:
                return {"is_open": True}

            def get_stock_live_market_context(
                self,
                *_args: object,
                **_kwargs: object,
            ) -> None:
                return None

            def place_order(self, *_args: object, **_kwargs: object) -> object:
                broker_calls.append("place_order")
                return object()

        class Strategy:
            def __init__(self, guarded_session_factory) -> None:  # noqa: ANN001
                self.guarded_session_factory = guarded_session_factory

            def run_cycle(self, *, adapter: object, **_kwargs: object) -> dict[str, object]:
                entered.set()
                assert release.wait(timeout=2)
                try:
                    adapter.place_order("token", object())  # type: ignore[attr-defined]
                except asyncio.CancelledError:
                    broker_boundary_blocked.set()
                else:
                    raise AssertionError("shutdown did not fence broker placement")
                with self.guarded_session_factory() as db:
                    db.execute(text("INSERT INTO shutdown_sentinel (value) VALUES (1)"))
                    commit_attempted.set()
                    db.commit()
                    commit_completed.set()
                return {"status": "healthy"}

            def snapshot(self) -> dict[str, object]:
                return {}

        if runtime_kind == "crypto":
            import app.services.alpaca_crypto_runtime as runtime_module

            runtime = AlpacaCryptoRuntimeCoordinator(
                settings=settings,
                session_factory=session_factory,
            )
        else:
            import app.services.alpaca_us_equity_runtime as runtime_module

            runtime = AlpacaUsEquityRuntimeCoordinator(
                settings=settings,
                session_factory=session_factory,
            )
        adapter = Adapter()
        monkeypatch.setattr(
            runtime_module,
            "build_broker_adapter",
            lambda *_args, **_kwargs: adapter,
        )
        monkeypatch.setattr(
            runtime_module,
            "ensure_authenticated_broker_session",
            lambda **_kwargs: SimpleNamespace(access_token="token"),
        )
        runtime._strategy = Strategy(  # type: ignore[assignment]
            runtime._cycle_session_factory
        )
        runtime._execution_scope = {
            "policy_version": EXECUTION_SCOPE_POLICY_VERSION,
            "provider": "alpaca",
            "account_scope": "a" * 24,
        }
        runtime._last_reconciled_at = datetime.now(UTC)
        runtime._shutdown_drain_timeout_seconds = lambda: 0.02  # type: ignore[method-assign]

        await runtime.start()
        try:
            while not entered.is_set():
                await asyncio.sleep(0.001)
            with pytest.raises(TimeoutError, match="in-flight cycle"):
                await runtime.stop()

            release.set()
            await asyncio.wait_for(runtime.wait_stopped(), timeout=1)

            with engine.connect() as connection:
                terminal_count = connection.scalar(
                    text("SELECT count(*) FROM shutdown_sentinel")
                )
            assert broker_calls == []
            assert broker_boundary_blocked.is_set() is True
            assert commit_attempted.is_set() is True
            assert commit_completed.is_set() is False
            assert terminal_count == 0
        finally:
            release.set()
            await runtime.wait_stopped()
            engine.dispose()

    asyncio.run(scenario())


def test_autonomous_cycle_released_after_stop_timeout_cannot_mutate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    async def scenario() -> None:
        settings = Settings(
            database_url=f"sqlite:///{tmp_path / 'autonomous-release.db'}",
            runtime_mode="paper",
            autonomous_runtime_enabled=True,
            nxt_eligible_sync_enabled=False,
        )
        engine = create_db_engine(settings)
        Base.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE shutdown_sentinel (value INTEGER)"))
        session_factory = create_session_factory(engine)
        entered = threading.Event()
        release = threading.Event()
        broker_boundary_blocked = threading.Event()
        commit_attempted = threading.Event()
        commit_completed = threading.Event()
        broker_calls: list[str] = []

        class Adapter:
            provider = "kis"

            def place_order(self, *_args: object, **_kwargs: object) -> object:
                broker_calls.append("place_order")
                return object()

        def delayed_dispatch(*, db, adapter, **_kwargs):  # noqa: ANN001, ANN202
            entered.set()
            assert release.wait(timeout=2)
            try:
                adapter.place_order("token", object())
            except asyncio.CancelledError:
                broker_boundary_blocked.set()
            else:
                raise AssertionError("shutdown did not fence broker placement")
            db.execute(text("INSERT INTO shutdown_sentinel (value) VALUES (1)"))
            commit_attempted.set()
            db.commit()
            commit_completed.set()
            return SimpleNamespace(status_code=200, payload={"completed": True})

        monkeypatch.setattr(
            "app.services.autonomous_runtime.dispatch_due_execution_jobs",
            delayed_dispatch,
        )
        runtime = AutonomousRuntimeCoordinator(
            settings=settings,
            session_factory=session_factory,
            broker_adapter_factory=(
                lambda _settings: Adapter()  # type: ignore[arg-type,return-value]
            ),
        )
        runtime._shutdown_drain_timeout_seconds = lambda: 0.02  # type: ignore[method-assign]
        runtime._dispatch_task = asyncio.create_task(
            runtime._run_dispatch_cycle(datetime.now(UTC))
        )
        try:
            while not entered.is_set():
                await asyncio.sleep(0.001)
            with pytest.raises(TimeoutError, match="in-flight cycle"):
                await runtime.stop()

            release.set()
            await asyncio.wait_for(runtime.wait_stopped(), timeout=1)

            with engine.connect() as connection:
                terminal_count = connection.scalar(
                    text("SELECT count(*) FROM shutdown_sentinel")
                )
            assert broker_calls == []
            assert broker_boundary_blocked.is_set() is True
            assert commit_attempted.is_set() is True
            assert commit_completed.is_set() is False
            assert terminal_count == 0
        finally:
            release.set()
            await runtime.wait_stopped()
            engine.dispose()

    asyncio.run(scenario())
