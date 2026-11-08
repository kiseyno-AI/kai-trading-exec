from __future__ import annotations

import asyncio

import pytest

from app.main import _shutdown_runtime_coordinators, _stop_runtime_coordinators


class _RecordingCoordinator:
    def __init__(
        self,
        *,
        name: str,
        calls: list[str],
        error: BaseException | None = None,
    ) -> None:
        self._name = name
        self._calls = calls
        self._error = error

    async def stop(self) -> None:
        await asyncio.sleep(0)
        self._calls.append(self._name)
        if self._error is not None:
            raise self._error


def test_stop_runtime_coordinators_attempts_every_stop_in_order() -> None:
    calls: list[str] = []
    coordinators = (
        (
            "alpaca_us_equity",
            _RecordingCoordinator(
                name="alpaca_us_equity",
                calls=calls,
                error=TimeoutError("US worker did not drain"),
            ),
        ),
        (
            "alpaca_crypto",
            _RecordingCoordinator(name="alpaca_crypto", calls=calls),
        ),
        (
            "autonomous",
            _RecordingCoordinator(name="autonomous", calls=calls),
        ),
    )

    with pytest.raises(ExceptionGroup) as exc_info:
        asyncio.run(_stop_runtime_coordinators(coordinators))

    assert calls == ["alpaca_us_equity", "alpaca_crypto", "autonomous"]
    assert len(exc_info.value.exceptions) == 1
    assert isinstance(exc_info.value.exceptions[0], TimeoutError)


def test_stop_runtime_coordinators_drains_after_cancellation_and_re_raises_it() -> None:
    calls: list[str] = []
    cancellation = asyncio.CancelledError("shutdown cancelled")
    coordinators = (
        (
            "alpaca_us_equity",
            _RecordingCoordinator(
                name="alpaca_us_equity",
                calls=calls,
                error=cancellation,
            ),
        ),
        (
            "alpaca_crypto",
            _RecordingCoordinator(
                name="alpaca_crypto",
                calls=calls,
                error=TimeoutError("crypto worker did not drain"),
            ),
        ),
        (
            "autonomous",
            _RecordingCoordinator(name="autonomous", calls=calls),
        ),
    )

    with pytest.raises(asyncio.CancelledError) as exc_info:
        asyncio.run(_stop_runtime_coordinators(coordinators))

    assert exc_info.value is cancellation
    assert calls == ["alpaca_us_equity", "alpaca_crypto", "autonomous"]
    assert isinstance(exc_info.value.__cause__, ExceptionGroup)
    assert len(exc_info.value.__cause__.exceptions) == 1
    assert isinstance(exc_info.value.__cause__.exceptions[0], TimeoutError)


def test_shutdown_preserves_admission_and_drain_failures() -> None:
    calls: list[str] = []
    admission_error = RuntimeError("admission fence failed")
    drain_error = TimeoutError("worker did not drain")

    class FailingCoordinator(_RecordingCoordinator):
        def begin_shutdown(self) -> None:
            calls.append("fence")
            raise admission_error

    coordinators = (
        ("failing", FailingCoordinator(name="failing", calls=calls, error=drain_error)),
        ("healthy", _RecordingCoordinator(name="healthy", calls=calls)),
    )
    with pytest.raises(ExceptionGroup) as exc_info:
        asyncio.run(_shutdown_runtime_coordinators(coordinators=coordinators))

    def leaf_failures(error: BaseException) -> list[BaseException]:
        if isinstance(error, BaseExceptionGroup):
            return [leaf for nested in error.exceptions for leaf in leaf_failures(nested)]
        return [error]

    assert calls == ["fence", "failing", "healthy"]
    assert leaf_failures(exc_info.value) == [admission_error, drain_error]
