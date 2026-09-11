from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import subprocess
import sys
from threading import Event, Thread, enumerate as enumerate_threads
import time

import pytest
from sqlalchemy import event, select

from app.broker import kis_token_cache as kis_token_cache_module
from app.broker import kis_token_coordinator as kis_token_coordinator_module
from app.broker.base import (
    BrokerAdapterError,
    BrokerErrorClassification,
    BrokerOrderPlacementRequest,
    BrokerRealtimeApproval,
    BrokerTokenGrant,
    BrokerTokenRevocation,
    NormalizedBrokerOrderStatus,
)
from app.broker.kis import JsonHttpResponse, KisBrokerAdapter
from app.broker.kis_token_cache import (
    CachedKISToken,
    CachedKISTokenPendingRevocation,
    CachedKISTokenRevocation,
    KISTokenCacheError,
    KISTokenRevocationClaimResult,
)
from app.broker.kis_token_coordinator import (
    SharedKISTokenRevocationClaim,
    build_kis_token_cache_store,
    invalidate_shared_kis_token,
    issue_shared_kis_token,
)
from app.core.config import Settings, load_settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import BrokerSession, BrokerSessionEvent
from app.services.broker_auth import (
    BrokerSessionCommand,
    authenticate_broker_session,
    build_broker_session_summary,
    ensure_authenticated_broker_session,
    ensure_realtime_approval,
    revoke_broker_session,
)
from app.services import broker_auth as broker_auth_service
from app.services.portfolio_risk import build_portfolio_risk_snapshot


class QueueHttpClient:
    def __init__(self, *responses: object) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def get_json(self, **kwargs: object) -> JsonHttpResponse:
        return self._respond(kwargs)

    def post_json(self, **kwargs: object) -> JsonHttpResponse:
        return self._respond(kwargs)

    def _respond(self, call: dict[str, object]) -> JsonHttpResponse:
        self.calls.append(call)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        assert isinstance(response, JsonHttpResponse)
        return response


def _settings(tmp_path: Path, *, runtime_mode: str = "paper") -> Settings:
    return Settings(
        database_url="sqlite:///:memory:",
        runtime_mode=runtime_mode,
        kis_app_key="shared-app-key",
        kis_app_secret="shared-app-secret",
        kis_token_cache_directory=str(tmp_path / "token-cache"),
        kis_token_request_timeout_seconds=5,
        kis_token_min_issue_interval_seconds=60,
        kis_token_issuance_lease_seconds=75,
    )


def _response(payload: dict[str, object], *, status_code: int = 200) -> JsonHttpResponse:
    return JsonHttpResponse(status_code=status_code, headers={}, payload=payload)


def _token_response(token: str) -> JsonHttpResponse:
    return _response(
        {
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": 3600,
        }
    )


def _database(settings: Settings):
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    return engine, create_session_factory(engine)


def test_token_cache_v1_matches_core_golden_identity_and_json_contract(
    tmp_path: Path,
) -> None:
    settings = Settings(
        runtime_mode="live",
        kis_app_key="test-app-key",
        kis_app_secret="test-app-secret",
        kis_token_cache_directory=str(tmp_path / "cache"),
    )
    store = build_kis_token_cache_store(settings)
    issued_at = datetime(2026, 9, 9, 1, 2, 3, tzinfo=UTC)
    record = store.save_token(
        access_token="interop-token",
        token_type="Bearer",
        issued_at=issued_at,
        expires_at=issued_at + timedelta(hours=24),
        refresh_after=issued_at + timedelta(hours=23, minutes=55),
    )

    expected_digest = (
        "ed79e64233bdae81aa466510d22091f34a2019beee1773c697db68151f10125e"
    )
    assert store.credential_fingerprint == f"hmac-sha256:{expected_digest}"
    assert store.token_path.name == f"real-{expected_digest[:32]}.token.json"
    assert store.load_token_record(now=issued_at) == record


@pytest.mark.parametrize(
    "save_kind",
    ("token", "issuance_attempt", "failure"),
)
def test_shared_cache_public_writes_reject_naive_datetimes(
    tmp_path: Path,
    save_kind: str,
) -> None:
    store = build_kis_token_cache_store(_settings(tmp_path))
    naive_now = datetime.now()
    if save_kind == "token":
        operation = lambda: store.save_token(
            access_token="naive-cache-token",
            token_type="Bearer",
            issued_at=naive_now,
            expires_at=naive_now + timedelta(hours=1),
            refresh_after=naive_now + timedelta(minutes=30),
        )
    elif save_kind == "issuance_attempt":
        operation = lambda: store.save_issuance_attempt(
            started_at=naive_now,
            cooldown_until=naive_now + timedelta(seconds=75),
            invalidates_generation=None,
        )
    else:
        operation = lambda: store.save_failure(
            error_kind="transport",
            status_code=503,
            retryable=True,
            failed_at=naive_now,
            cooldown_until=naive_now + timedelta(seconds=60),
        )

    with pytest.raises(KISTokenCacheError, match="timezone-aware"):
        operation()


@pytest.mark.parametrize(
    "record_type",
    (
        "token",
        "pending_revocation",
        "revocation",
        "issuance_attempt",
        "failure",
        "issuance_success",
    ),
)
def test_shared_cache_records_reject_unknown_keys(
    tmp_path: Path,
    record_type: str,
) -> None:
    store = build_kis_token_cache_store(_settings(tmp_path))
    now = datetime.now(UTC)
    if record_type in {"token", "pending_revocation", "revocation"}:
        record = store.save_token(
            access_token="strict-schema-token",
            token_type="Bearer",
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=30),
        )
        if record_type == "pending_revocation":
            assert asyncio.run(
                store.begin_revocation_if_matches(
                    access_token=record.access_token,
                    generation=record.generation,
                )
            )
        elif record_type == "revocation":
            assert asyncio.run(
                store.invalidate_if_matches(
                    access_token=record.access_token,
                    generation=record.generation,
                    minimum_issue_interval_seconds=60,
                )
            )
        path = store.token_path
        load = store.load_cache_state
    elif record_type == "issuance_attempt":
        store.save_issuance_attempt(
            started_at=now,
            cooldown_until=now + timedelta(seconds=75),
            invalidates_generation=None,
        )
        path = store.failure_path
        load = lambda: store.load_failure(include_expired=True)
    elif record_type == "failure":
        store.save_failure(
            error_kind="transport",
            status_code=503,
            retryable=True,
            failed_at=now,
            cooldown_until=now + timedelta(seconds=60),
        )
        path = store.failure_path
        load = lambda: store.load_failure(include_expired=True)
    else:
        store.save_token_after_issuance(
            access_token="strict-success-token",
            token_type="Bearer",
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=30),
            minimum_issue_interval_seconds=60,
        )
        path = store.failure_path
        load = lambda: store.load_failure(include_expired=True)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["unexpected"] = "schema drift"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(KISTokenCacheError, match="fields are invalid"):
        load()


def test_settings_load_shared_kis_token_cache_contract(tmp_path: Path) -> None:
    settings = load_settings(
        {
            "KAI_EXEC_KIS_TOKEN_CACHE_DIRECTORY": str(tmp_path / "cache"),
            "KAI_EXEC_KIS_TOKEN_REFRESH_MARGIN_SECONDS": "240",
            "KAI_EXEC_KIS_TOKEN_FAILURE_COOLDOWN_SECONDS": "75",
            "KAI_EXEC_KIS_TOKEN_REQUEST_TIMEOUT_SECONDS": "15",
            "KAI_EXEC_KIS_TOKEN_MIN_ISSUE_INTERVAL_SECONDS": "60",
            "KAI_EXEC_KIS_TOKEN_ISSUANCE_LEASE_SECONDS": "75",
        }
    )

    assert settings.kis_token_cache_directory == str(tmp_path / "cache")
    assert settings.kis_token_refresh_margin_seconds == 240
    assert settings.kis_token_failure_cooldown_seconds == 75
    assert settings.kis_token_request_timeout_seconds == 15
    assert settings.kis_token_min_issue_interval_seconds == 60
    assert settings.kis_token_issuance_lease_seconds == 75


def test_revocation_http_timeout_must_fit_inside_token_claim_lease(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ValueError,
        match="larger of KIS_TOKEN_REQUEST_TIMEOUT_SECONDS and KIS_REQUEST_TIMEOUT_SECONDS",
    ):
        Settings(
            database_url="sqlite:///:memory:",
            runtime_mode="paper",
            kis_app_key="shared-app-key",
            kis_app_secret="shared-app-secret",
            kis_token_cache_directory=str(tmp_path / "token-cache"),
            kis_request_timeout_seconds=120,
            kis_token_request_timeout_seconds=5,
            kis_token_min_issue_interval_seconds=60,
            kis_token_issuance_lease_seconds=75,
        )


@pytest.mark.parametrize(
    ("runtime_mode", "environment"),
    (("live", "real"), ("paper", "demo")),
)
def test_cache_environment_is_derived_from_runtime_mode(
    tmp_path: Path,
    runtime_mode: str,
    environment: str,
) -> None:
    store = build_kis_token_cache_store(_settings(tmp_path, runtime_mode=runtime_mode))

    assert store.environment == environment


def test_mock_runtime_cannot_construct_a_shared_kis_token_store(tmp_path: Path) -> None:
    with pytest.raises(BrokerAdapterError, match="mock runtime"):
        build_kis_token_cache_store(_settings(tmp_path, runtime_mode="mock"))


def test_shared_token_is_authoritative_and_db_is_only_a_mirror(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    client = QueueHttpClient(_token_response("shared-token-1"))
    adapter = KisBrokerAdapter(settings=settings, http_client=client)
    engine, session_factory = _database(settings)

    with session_factory() as db:
        first = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
        )
        first_generation = first.generation
    with session_factory() as db:
        state = db.get(BrokerSession, "kis:paper")
        assert state is not None
        state.access_token = "stale-db-token"
        db.commit()
    with session_factory() as db:
        reused = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
        )

        assert reused.access_token == "shared-token-1"
        assert reused.generation == first_generation
        assert reused.refresh_after is not None
    assert len(client.calls) == 1
    assert client.calls[0]["timeout_seconds"] == 5
    engine.dispose()


def test_corrupt_shared_state_never_falls_back_to_reusable_db_token(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    store.directory.mkdir(mode=0o700, parents=True)
    store.token_path.write_text("{not-json", encoding="utf-8")
    store.token_path.chmod(0o600)
    client = QueueHttpClient(_token_response("must-not-be-used"))
    adapter = KisBrokerAdapter(settings=settings, http_client=client)
    engine, session_factory = _database(settings)

    with session_factory() as db:
        db.add(
            BrokerSession(
                id="kis:paper",
                provider="kis",
                mode="paper",
                access_token="reusable-db-token",
                access_token_expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
        )
        db.commit()
    with session_factory() as db:
        with pytest.raises(BrokerAdapterError, match="not safe"):
            ensure_authenticated_broker_session(
                settings=settings,
                db=db,
                adapter=adapter,
            )

    assert client.calls == []
    engine.dispose()


def test_portfolio_risk_does_not_bypass_corrupt_shared_state_with_db_token(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    store.directory.mkdir(mode=0o700, parents=True)
    store.token_path.write_text("{not-json", encoding="utf-8")
    store.token_path.chmod(0o600)
    client = QueueHttpClient(_token_response("must-not-be-used"))
    adapter = KisBrokerAdapter(settings=settings, http_client=client)
    engine, session_factory = _database(settings)
    with session_factory() as db:
        db.add(
            BrokerSession(
                id="kis:paper",
                provider="kis",
                mode="paper",
                access_token="reusable-db-token",
                access_token_expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
        )
        db.commit()
        payload = build_portfolio_risk_snapshot(
            settings=settings,
            db=db,
            adapter=adapter,
            market_scope="krx",
        )

    assert payload["account"]["snapshot_status"] == "unavailable"
    assert "shared token cache" in str(payload["account"]["snapshot_reason"])
    assert client.calls == []
    engine.dispose()


@pytest.mark.parametrize(
    "shared_state",
    ("missing", "pending_revocation", "revoked", "corrupt"),
)
def test_session_summary_never_reports_stale_db_token_as_active(
    tmp_path: Path,
    shared_state: str,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    if shared_state in {"pending_revocation", "revoked"}:
        now = datetime.now(UTC)
        record = store.save_token(
            access_token="summary-token",
            token_type="Bearer",
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=30),
        )
        if shared_state == "pending_revocation":
            assert asyncio.run(
                store.begin_revocation_if_matches(
                    access_token=record.access_token,
                    generation=record.generation,
                )
            )
        else:
            assert invalidate_shared_kis_token(
                settings=settings,
                access_token=record.access_token,
                generation=record.generation,
            )
    elif shared_state == "corrupt":
        store.directory.mkdir(mode=0o700, parents=True)
        store.token_path.write_text("{bad", encoding="utf-8")
        store.token_path.chmod(0o600)
    adapter = KisBrokerAdapter(settings=settings, http_client=QueueHttpClient())
    engine, session_factory = _database(settings)
    with session_factory() as db:
        db.add(
            BrokerSession(
                id="kis:paper",
                provider="kis",
                mode="paper",
                access_token="stale-db-token",
                access_token_expires_at=datetime.now(UTC) + timedelta(hours=1),
                refresh_token="stale-refresh-token",
                refresh_token_expires_at=datetime.now(UTC) + timedelta(hours=2),
                approval_key="stale-approval-key",
                approval_key_issued_at=datetime.now(UTC),
            )
        )
        db.commit()
        summary = build_broker_session_summary(
            settings=settings,
            db=db,
            adapter=adapter,
        )

    assert summary["status"] == {
        "missing": "uninitialized",
        "pending_revocation": "error",
        "revoked": "revoked",
        "corrupt": "error",
    }[shared_state]
    assert summary["access_token_present"] is False
    assert summary["approval_key_present"] is False
    assert summary["approval_key_issued_at"] is None
    assert summary["refresh_token_present"] is False
    assert summary["refresh_token_expires_at"] is None
    assert "access_token" not in summary
    engine.dispose()


@pytest.mark.parametrize(
    "guard_kind",
    ("pending_revocation", "revocation", "attempt", "failure"),
)
def test_shared_guard_never_falls_back_to_reusable_db_token(
    tmp_path: Path,
    guard_kind: str,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    if guard_kind in {"pending_revocation", "revocation"}:
        record = store.save_token_after_issuance(
            access_token="guarded-token",
            token_type="Bearer",
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            refresh_after=now + timedelta(minutes=30),
            minimum_issue_interval_seconds=60,
        )
        if guard_kind == "pending_revocation":
            assert asyncio.run(
                store.begin_revocation_if_matches(
                    access_token=record.access_token,
                    generation=record.generation,
                )
            )
        else:
            assert invalidate_shared_kis_token(
                settings=settings,
                access_token=record.access_token,
                generation=record.generation,
            )
    elif guard_kind == "attempt":
        store.save_issuance_attempt(
            started_at=now,
            cooldown_until=now + timedelta(seconds=75),
            invalidates_generation=None,
        )
    else:
        store.save_failure(
            error_kind="transport",
            status_code=503,
            retryable=True,
            failed_at=now,
            cooldown_until=now + timedelta(seconds=60),
        )
    client = QueueHttpClient(_token_response("must-not-be-issued"))
    adapter = KisBrokerAdapter(settings=settings, http_client=client)
    engine, session_factory = _database(settings)
    with session_factory() as db:
        db.add(
            BrokerSession(
                id="kis:paper",
                provider="kis",
                mode="paper",
                access_token="reusable-db-token",
                access_token_expires_at=now + timedelta(hours=1),
            )
        )
        db.commit()
    with session_factory() as db:
        with pytest.raises(BrokerAdapterError, match="suppressed"):
            ensure_authenticated_broker_session(
                settings=settings,
                db=db,
                adapter=adapter,
            )

    assert client.calls == []
    engine.dispose()


def test_actual_subprocess_waits_for_cache_lock_and_reads_same_token(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    store.save_token(
        access_token="cross-process-token",
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
    )
    marker = tmp_path / "child-result.txt"
    child_source = """
import asyncio
from pathlib import Path
import sys
from app.broker.kis_token_cache import KISTokenCacheStore

async def main():
    store = KISTokenCacheStore(
        directory=sys.argv[1],
        environment='demo',
        app_key='shared-app-key',
        app_secret='shared-app-secret',
        minimum_issue_interval_seconds=60,
        issuance_lease_seconds=75,
    )
    async with store.lock(poll_interval_seconds=0.005):
        token = store.load_token()
        Path(sys.argv[2]).write_text(token.access_token, encoding='utf-8')

asyncio.run(main())
"""
    descriptor = store.open_lock()
    assert store.try_acquire_lock(descriptor)
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            child_source,
            str(store.directory),
            str(marker),
        ],
        cwd=Path(__file__).resolve().parents[1] / "apps" / "api",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(0.15)
    assert marker.exists() is False
    store.release_lock(descriptor)
    stdout, stderr = process.communicate(timeout=3)

    assert process.returncode == 0, (stdout, stderr)
    assert marker.read_text(encoding="utf-8") == "cross-process-token"


def test_async_cache_lock_contention_times_out_within_lease_and_closes_waiter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = build_kis_token_cache_store(_settings(tmp_path))
    store.issuance_lease_seconds = 0.03
    held_descriptor = store.open_lock()
    assert store.try_acquire_lock(held_descriptor)
    released_descriptors: list[int] = []
    original_release_lock = store.release_lock

    def record_release(descriptor: int) -> None:
        released_descriptors.append(descriptor)
        original_release_lock(descriptor)

    monkeypatch.setattr(store, "release_lock", record_release)

    async def contend() -> None:
        async with store.lock(poll_interval_seconds=0.005):
            raise AssertionError("contended lock must not be acquired")

    started_at = time.monotonic()
    try:
        with pytest.raises(KISTokenCacheError, match="lock timed out"):
            asyncio.run(asyncio.wait_for(contend(), timeout=0.3))
    finally:
        original_release_lock(held_descriptor)

    assert time.monotonic() - started_at < 0.3
    assert len(released_descriptors) == 1


def test_sync_revocation_claim_finishes_started_write_before_return_without_workers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    record = store.save_token(
        access_token="slow-claim-private-token",
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
    )
    write_completed = Event()
    original_write = store._write_json

    def slow_write(*args: object, **kwargs: object) -> None:
        time.sleep(0.1)
        original_write(*args, **kwargs)
        write_completed.set()

    monkeypatch.setattr(store, "_write_json", slow_write)
    monkeypatch.setattr(
        kis_token_coordinator_module,
        "build_kis_token_cache_store",
        lambda _settings: store,
    )

    def reject_async_worker(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("synchronous coordinator must not spawn an async cache worker")

    monkeypatch.setattr(
        kis_token_cache_module,
        "run_token_cache_io",
        reject_async_worker,
    )

    existing_thread_ids = {thread.ident for thread in enumerate_threads()}
    started_at = time.monotonic()
    claim = kis_token_coordinator_module.begin_shared_kis_token_revocation(
        settings=settings,
        access_token=record.access_token,
        generation=record.generation,
    )
    elapsed = time.monotonic() - started_at

    assert elapsed >= 0.09
    assert claim.outcome == KISTokenRevocationClaimResult.ACQUIRED
    assert claim.claim_id is not None
    assert write_completed.is_set()
    pending = store.load_cache_state()
    assert isinstance(pending, CachedKISTokenPendingRevocation)
    assert pending.claim_id == claim.claim_id
    assert [
        thread
        for thread in enumerate_threads()
        if thread.ident not in existing_thread_ids and thread.is_alive()
    ] == []


def test_sync_revocation_claim_lock_timeout_has_no_late_mutation_or_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    store.issuance_lease_seconds = 0.03
    now = datetime.now(UTC)
    record = store.save_token(
        access_token="contended-claim-private-token",
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
    )
    monkeypatch.setattr(
        kis_token_coordinator_module,
        "build_kis_token_cache_store",
        lambda _settings: store,
    )
    held_descriptor = store.open_lock()
    assert store.try_acquire_lock(held_descriptor)
    existing_thread_ids = {thread.ident for thread in enumerate_threads()}

    started_at = time.monotonic()
    errors: list[BrokerAdapterError] = []
    try:
        for _attempt in range(3):
            with pytest.raises(BrokerAdapterError, match="claim timed out") as captured:
                kis_token_coordinator_module.begin_shared_kis_token_revocation(
                    settings=settings,
                    access_token=record.access_token,
                    generation=record.generation,
                )
            errors.append(captured.value)
    finally:
        store.release_lock(held_descriptor)
    elapsed = time.monotonic() - started_at

    assert elapsed < 0.3
    assert len(errors) == 3
    assert all(error.status_code == 503 for error in errors)
    assert all(error.retryable is True for error in errors)
    assert all(error.code is None for error in errors)
    assert all(error.response_metadata == {} for error in errors)
    time.sleep(0.05)
    assert store.load_cache_state() == record
    assert [
        thread
        for thread in enumerate_threads()
        if thread.ident not in existing_thread_ids and thread.is_alive()
    ] == []


def test_public_cache_lock_timeout_is_retryable_and_does_not_leak_secrets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    store.issuance_lease_seconds = 0.03
    private_token = "private-timeout-token"
    now = datetime.now(UTC)
    record = store.save_token(
        access_token=private_token,
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
    )
    monkeypatch.setattr(
        kis_token_coordinator_module,
        "build_kis_token_cache_store",
        lambda _settings: store,
    )
    held_descriptor = store.open_lock()
    assert store.try_acquire_lock(held_descriptor)

    async def invalidate_inside_running_loop() -> BrokerAdapterError:
        try:
            invalidate_shared_kis_token(
                settings=settings,
                access_token=private_token,
                generation=record.generation,
            )
        except BrokerAdapterError as exc:
            return exc
        raise AssertionError("cache contention must reject invalidation")

    try:
        error = asyncio.run(invalidate_inside_running_loop())
    finally:
        store.release_lock(held_descriptor)

    assert error.status_code == 503
    assert error.retryable is True
    assert error.classification == BrokerErrorClassification.TRANSIENT
    assert error.normalized_status == NormalizedBrokerOrderStatus.RETRY_PENDING
    public_error = json.dumps(
        {
            "message": str(error),
            "repr": repr(error),
            "request_metadata": error.request_metadata,
            "response_metadata": error.response_metadata,
            "request_payload": error.request_payload,
            "response_payload": error.response_payload,
        }
    )
    assert private_token not in public_error
    assert settings.kis_app_secret not in public_error


def test_force_refresh_adopts_peer_generation_without_second_issue(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    first_started = Event()
    release_first = Event()

    class BlockingClient(QueueHttpClient):
        def _respond(self, call: dict[str, object]) -> JsonHttpResponse:
            self.calls.append(call)
            first_started.set()
            assert release_first.wait(timeout=3)
            return _token_response("peer-token")

    seed_store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    seed_store.save_token_after_issuance(
        access_token="seed-token",
        token_type="Bearer",
        issued_at=now - timedelta(minutes=2),
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
        minimum_issue_interval_seconds=60,
    )
    client = BlockingClient()
    first_adapter = KisBrokerAdapter(settings=settings, http_client=client)
    second_adapter = KisBrokerAdapter(settings=settings, http_client=client)
    outcomes: list[str] = []

    def refresh(adapter: KisBrokerAdapter) -> None:
        grant = issue_shared_kis_token(
            settings=settings,
            issue_token=adapter.authenticate,
            force=True,
        )
        outcomes.append(str(grant.generation))

    first_thread = Thread(target=refresh, args=(first_adapter,))
    second_thread = Thread(target=refresh, args=(second_adapter,))
    first_thread.start()
    assert first_started.wait(timeout=3)
    second_thread.start()
    release_first.set()
    first_thread.join(timeout=5)
    second_thread.join(timeout=5)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert len(client.calls) == 1
    assert len(outcomes) == 2
    assert outcomes[0] == outcomes[1]


def test_concurrent_first_authentication_mirrors_one_shared_generation(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path).model_copy(
        update={"database_url": f"sqlite:///{tmp_path / 'concurrent-mirror.db'}"}
    )
    first_started = Event()
    release_first = Event()

    class BlockingClient(QueueHttpClient):
        def _respond(self, call: dict[str, object]) -> JsonHttpResponse:
            self.calls.append(call)
            first_started.set()
            assert release_first.wait(timeout=3)
            return _token_response("concurrent-shared-token")

    client = BlockingClient()
    adapter = KisBrokerAdapter(settings=settings, http_client=client)
    engine, session_factory = _database(settings)
    generations: list[str | None] = []
    errors: list[BaseException] = []

    def authenticate() -> None:
        try:
            with session_factory() as db:
                state = ensure_authenticated_broker_session(
                    settings=settings,
                    db=db,
                    adapter=adapter,
                )
                generations.append(state.generation)
        except BaseException as exc:
            errors.append(exc)

    first_thread = Thread(target=authenticate)
    second_thread = Thread(target=authenticate)
    first_thread.start()
    assert first_started.wait(timeout=3)
    second_thread.start()
    release_first.set()
    first_thread.join(timeout=5)
    second_thread.join(timeout=5)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert errors == []
    assert len(client.calls) == 1
    assert len(generations) == 2
    assert generations[0] == generations[1]
    engine.dispose()


def test_delayed_token_grant_cannot_overwrite_newer_database_mirror(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    adapter = KisBrokerAdapter(settings=settings, http_client=QueueHttpClient())
    engine, session_factory = _database(settings)
    now = datetime.now(UTC)
    delayed_grant = BrokerTokenGrant(
        access_token="delayed-token",
        token_type="Bearer",
        access_token_expires_at=now + timedelta(hours=1),
        generation="00000000-0000-0000-0000-000000000001",
        refresh_after=now + timedelta(minutes=30),
    )
    authoritative_grant = BrokerTokenGrant(
        access_token="authoritative-token",
        token_type="Bearer",
        access_token_expires_at=now + timedelta(hours=2),
        generation="00000000-0000-0000-0000-000000000002",
        refresh_after=now + timedelta(minutes=90),
    )
    grants = iter((delayed_grant, authoritative_grant))
    monkeypatch.setattr(
        broker_auth_service,
        "_authenticate_grant",
        lambda **_: next(grants),
    )

    with session_factory() as db:
        db.add(
            BrokerSession(
                id="kis:paper",
                provider="kis",
                mode="paper",
                api_base_url=adapter.describe().api_base_url,
                access_token=authoritative_grant.access_token,
                token_type=authoritative_grant.token_type,
                access_token_expires_at=authoritative_grant.access_token_expires_at,
                generation=authoritative_grant.generation,
                refresh_after=authoritative_grant.refresh_after,
            )
        )
        db.commit()

        state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
        )

        assert state.generation == authoritative_grant.generation
        assert state.access_token == authoritative_grant.access_token
    engine.dispose()


def test_realtime_approval_is_never_attached_to_a_different_token_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    adapter = KisBrokerAdapter(settings=settings, http_client=QueueHttpClient())
    engine, session_factory = _database(settings)
    now = datetime.now(UTC)

    with session_factory() as db:
        state = BrokerSession(
            id="kis:paper",
            provider="kis",
            mode="paper",
            api_base_url=adapter.describe().api_base_url,
            access_token="first-generation-token",
            token_type="Bearer",
            access_token_expires_at=now + timedelta(hours=1),
            generation="00000000-0000-0000-0000-000000000001",
            refresh_after=now + timedelta(minutes=30),
        )
        db.add(state)
        db.commit()
        issued_for: list[str] = []

        monkeypatch.setattr(
            broker_auth_service,
            "ensure_authenticated_broker_session",
            lambda **_: state,
        )

        def current_grant(**_: object) -> BrokerTokenGrant:
            return BrokerTokenGrant(
                access_token=str(state.access_token),
                token_type=str(state.token_type),
                access_token_expires_at=state.access_token_expires_at or now,
                generation=state.generation,
                refresh_after=state.refresh_after,
            )

        monkeypatch.setattr(
            broker_auth_service,
            "_authenticate_grant",
            current_grant,
        )

        def issue_approval(access_token: str) -> BrokerRealtimeApproval:
            issued_for.append(access_token)
            if len(issued_for) == 1:
                state.access_token = "second-generation-token"
                state.access_token_expires_at = now + timedelta(hours=2)
                state.generation = "00000000-0000-0000-0000-000000000002"
                state.refresh_after = now + timedelta(minutes=90)
                db.flush()
                return BrokerRealtimeApproval(
                    approval_key="stale-generation-approval",
                    issued_at=now,
                )
            return BrokerRealtimeApproval(
                approval_key="current-generation-approval",
                issued_at=now + timedelta(seconds=1),
            )

        monkeypatch.setattr(adapter, "issue_realtime_approval", issue_approval)

        approved = ensure_realtime_approval(
            settings=settings,
            db=db,
            adapter=adapter,
        )

        assert issued_for == [
            "first-generation-token",
            "second-generation-token",
        ]
        assert approved.generation == "00000000-0000-0000-0000-000000000002"
        assert approved.approval_key == "current-generation-approval"
    engine.dispose()


def test_refresh_required_token_is_reused_during_minimum_issue_interval(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    record = store.save_token_after_issuance(
        access_token="minimum-interval-token",
        token_type="Bearer",
        issued_at=now - timedelta(seconds=30),
        expires_at=now + timedelta(hours=1),
        refresh_after=now - timedelta(seconds=1),
        minimum_issue_interval_seconds=60,
    )
    issue_calls = 0

    def issue_token():
        nonlocal issue_calls
        issue_calls += 1
        raise AssertionError("minimum issuance interval must suppress refresh")

    grant = issue_shared_kis_token(settings=settings, issue_token=issue_token)

    assert grant.access_token == record.access_token
    assert grant.generation == record.generation
    assert issue_calls == 0


def test_db_commit_failure_does_not_roll_back_shared_token(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    client = QueueHttpClient(_token_response("durable-shared-token"))
    adapter = KisBrokerAdapter(settings=settings, http_client=client)
    engine, session_factory = _database(settings)

    with session_factory() as db:
        def reject_commit(_session) -> None:
            raise RuntimeError("database unavailable")

        event.listen(db, "before_commit", reject_commit, once=True)
        with pytest.raises(RuntimeError, match="database unavailable"):
            ensure_authenticated_broker_session(
                settings=settings,
                db=db,
                adapter=adapter,
            )

    record = build_kis_token_cache_store(settings).load_token()
    assert isinstance(record, CachedKISToken)
    assert record.access_token == "durable-shared-token"
    engine.dispose()


def test_naive_token_expiry_from_issuer_is_fail_closed(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    with pytest.raises(BrokerAdapterError, match="invalid token payload"):
        issue_shared_kis_token(
            settings=settings,
            issue_token=lambda: BrokerTokenGrant(
                access_token="naive-expiry-token",
                token_type="Bearer",
                access_token_expires_at=datetime.now() + timedelta(hours=12),
            ),
        )

    assert build_kis_token_cache_store(settings).load_token() is None


@pytest.mark.parametrize(
    "expires_in",
    (float("inf"), float("nan"), 3600.5, True, "3600 ", "999999"),
)
def test_noncanonical_token_expiry_is_fail_closed_and_cooldown_is_shared(
    tmp_path: Path,
    expires_in: object,
) -> None:
    settings = _settings(tmp_path)
    client = QueueHttpClient(
        _response(
            {
                "access_token": "invalid-expiry-token",
                "token_type": "Bearer",
                "expires_in": expires_in,
            }
        )
    )
    adapter = KisBrokerAdapter(settings=settings, http_client=client)

    with pytest.raises(BrokerAdapterError):
        issue_shared_kis_token(
            settings=settings,
            issue_token=adapter.authenticate,
        )

    store = build_kis_token_cache_store(settings)
    assert store.load_token() is None
    assert store.load_failure() is not None


@pytest.mark.parametrize(
    "absolute_expiry",
    (
        "2026-09-09T10:00:00+09:00",
        "2026-9-9 1:2:3",
        "2026-02-30 10:00:00",
        123,
    ),
)
def test_malformed_provider_token_expiry_is_fail_closed(
    tmp_path: Path,
    absolute_expiry: object,
) -> None:
    settings = _settings(tmp_path)
    adapter = KisBrokerAdapter(
        settings=settings,
        http_client=QueueHttpClient(
            _response(
                {
                    "access_token": "invalid-absolute-expiry-token",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "access_token_token_expired": absolute_expiry,
                }
            )
        ),
    )

    with pytest.raises(BrokerAdapterError):
        issue_shared_kis_token(
            settings=settings,
            issue_token=adapter.authenticate,
        )

    assert build_kis_token_cache_store(settings).load_token() is None


def test_provider_token_expiry_is_a_conservative_absolute_bound(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    provider_expiry = (datetime.now(UTC) + timedelta(minutes=10)).replace(
        microsecond=0
    )
    provider_expiry_text = provider_expiry.astimezone(
        timezone(timedelta(hours=9))
    ).strftime("%Y-%m-%d %H:%M:%S")
    adapter = KisBrokerAdapter(
        settings=settings,
        http_client=QueueHttpClient(
            _response(
                {
                    "access_token": "absolute-bound-token",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "access_token_token_expired": provider_expiry_text,
                }
            )
        ),
    )

    grant = issue_shared_kis_token(
        settings=settings,
        issue_token=adapter.authenticate,
    )

    assert grant.access_token_expires_at == provider_expiry


@pytest.mark.parametrize(
    "rejection",
    (
        BrokerAdapterError(
            "unauthorized",
            status_code=422,
            response_metadata={"status_code": 401},
        ),
        _response({"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "expired"}),
    ),
)
def test_authenticated_rejection_cas_invalidates_without_replay(
    tmp_path: Path,
    rejection: object,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    store.save_token_after_issuance(
        access_token="rejected-token",
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
        minimum_issue_interval_seconds=60,
    )
    client = QueueHttpClient(rejection)
    adapter = KisBrokerAdapter(settings=settings, http_client=client)

    with pytest.raises(BrokerAdapterError):
        adapter.get_current_price("rejected-token", symbol="005930")

    assert len(client.calls) == 1
    assert isinstance(store.load_cache_state(), CachedKISTokenRevocation)


def test_stale_401_cannot_invalidate_a_peer_generation(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    store.save_token(
        access_token="stale-request-token",
        token_type="Bearer",
        issued_at=now - timedelta(minutes=2),
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
    )

    class PeerRefreshThenReject(QueueHttpClient):
        def _respond(self, call: dict[str, object]) -> JsonHttpResponse:
            self.calls.append(call)
            peer_time = datetime.now(UTC)
            store.save_token(
                access_token="peer-token",
                token_type="Bearer",
                issued_at=peer_time,
                expires_at=peer_time + timedelta(hours=1),
                refresh_after=peer_time + timedelta(minutes=30),
            )
            raise BrokerAdapterError(
                "stale unauthorized response",
                status_code=422,
                response_metadata={"status_code": 401},
            )

    client = PeerRefreshThenReject()
    adapter = KisBrokerAdapter(settings=settings, http_client=client)

    with pytest.raises(BrokerAdapterError):
        adapter.get_current_price("stale-request-token", symbol="005930")

    state = store.load_cache_state()
    assert isinstance(state, CachedKISToken)
    assert state.access_token == "peer-token"
    assert len(client.calls) == 1


def test_invalidation_requires_every_supplied_cas_predicate_to_match(
    tmp_path: Path,
) -> None:
    store = build_kis_token_cache_store(_settings(tmp_path))
    now = datetime.now(UTC)
    stale = store.save_token(
        access_token="provider-reused-token",
        token_type="Bearer",
        issued_at=now - timedelta(minutes=2),
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
    )
    current = store.save_token(
        access_token=stale.access_token,
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
    )

    invalidated = asyncio.run(
        store.invalidate_if_matches(
            generation=stale.generation,
            access_token=stale.access_token,
            minimum_issue_interval_seconds=60,
        )
    )

    assert invalidated is False
    assert store.load_cache_state() == current


@pytest.mark.parametrize("reject_at", ("hashkey", "order"))
def test_authenticated_post_or_hashkey_401_invalidates_without_replay(
    tmp_path: Path,
    reject_at: str,
) -> None:
    settings = _settings(tmp_path).model_copy(
        update={"kis_account_number": "12345678"}
    )
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    store.save_token_after_issuance(
        access_token="post-token",
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
        minimum_issue_interval_seconds=60,
    )
    unauthorized = BrokerAdapterError(
        "unauthorized",
        status_code=422,
        response_metadata={"status_code": 401},
    )
    responses = (
        (unauthorized,)
        if reject_at == "hashkey"
        else (_response({"HASH": "hash-key"}), unauthorized)
    )
    client = QueueHttpClient(*responses)
    adapter = KisBrokerAdapter(settings=settings, http_client=client)

    with pytest.raises(BrokerAdapterError):
        adapter.place_order(
            "post-token",
            BrokerOrderPlacementRequest(
                idempotency_key="order-once",
                symbol="005930",
                side="buy",
                quantity=1,
                order_type="limit",
                limit_price=Decimal("71000"),
                time_in_force="day",
                exchange="KRX",
            ),
        )

    assert len(client.calls) == (1 if reject_at == "hashkey" else 2)
    assert isinstance(store.load_cache_state(), CachedKISTokenRevocation)


def test_generation_change_clears_realtime_approval(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    client = QueueHttpClient(_token_response("new-generation-token"))
    adapter = KisBrokerAdapter(settings=settings, http_client=client)
    engine, session_factory = _database(settings)

    with session_factory() as db:
        db.add(
            BrokerSession(
                id="kis:paper",
                provider="kis",
                mode="paper",
                access_token="old-token",
                access_token_expires_at=datetime.now(UTC) + timedelta(hours=1),
                generation="00000000-0000-0000-0000-000000000001",
                refresh_after=datetime.now(UTC) + timedelta(minutes=30),
                approval_key="old-approval",
                approval_key_issued_at=datetime.now(UTC),
            )
        )
        db.commit()
        result = authenticate_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            command=BrokerSessionCommand(force=True),
        )

        assert result.payload["session"]["approval_key_present"] is False
        state = db.get(BrokerSession, "kis:paper")
        assert state is not None
        assert state.approval_key is None
    engine.dispose()


def test_explicit_revoke_failure_keeps_a_private_retry_target_behind_local_fence(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    record = store.save_token_after_issuance(
        access_token="revoked-token",
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
        minimum_issue_interval_seconds=60,
    )
    remote_error = BrokerAdapterError("remote revoke unavailable", status_code=503)
    client = QueueHttpClient(
        remote_error,
        _response({"message": "token revoked"}),
    )
    adapter = KisBrokerAdapter(settings=settings, http_client=client)
    engine, session_factory = _database(settings)

    with session_factory() as db:
        db.add(
            BrokerSession(
                id="kis:paper",
                provider="kis",
                mode="paper",
                access_token=record.access_token,
                access_token_expires_at=record.expires_at,
                generation=record.generation,
                refresh_after=record.refresh_after,
            )
        )
        db.commit()
        first_result = revoke_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            command=BrokerSessionCommand(),
        )

    assert first_result.status_code == 502
    pending = store.load_cache_state()
    assert isinstance(pending, CachedKISTokenPendingRevocation)
    assert pending.access_token == record.access_token
    assert pending.generation == record.generation
    assert store.load_token() is None
    assert len(client.calls) == 1
    with session_factory() as db:
        state = db.get(BrokerSession, "kis:paper")
        assert state is not None
        assert state.access_token is None
        assert state.last_revoked_at is None

        in_progress_result = revoke_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            command=BrokerSessionCommand(),
        )

    assert in_progress_result.status_code == 502
    assert in_progress_result.payload["outcome"] == "rejected"
    assert len(client.calls) == 1
    pending_payload = json.loads(store.token_path.read_text(encoding="utf-8"))
    expired_claimed_at = (
        datetime.now(UTC)
        - timedelta(seconds=store.issuance_lease_seconds + 1)
    )
    pending_payload["revocation_started_at"] = expired_claimed_at.isoformat()
    pending_payload["lease_expires_at"] = (
        expired_claimed_at
        + timedelta(seconds=store.issuance_lease_seconds)
    ).isoformat()
    store._write_json(store.token_path, pending_payload)

    with session_factory() as db:
        retry_result = revoke_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            command=BrokerSessionCommand(),
        )

    assert retry_result.status_code == 200
    assert retry_result.payload["outcome"] == "applied"
    assert isinstance(store.load_cache_state(), CachedKISTokenRevocation)
    assert [call["payload"]["token"] for call in client.calls] == [
        record.access_token,
        record.access_token,
    ]
    assert record.access_token not in json.dumps(first_result.payload)
    assert record.access_token not in json.dumps(retry_result.payload)
    with session_factory() as db:
        state = db.get(BrokerSession, "kis:paper")
        assert state is not None
        assert state.access_token is None
        assert state.last_revoked_at is not None
        events = db.scalars(select(BrokerSessionEvent)).all()
        public_event_fields = [
            {
                "message": event.message,
                "reason": event.reason,
                "previous_state": event.previous_state,
                "current_state": event.current_state,
            }
            for event in events
        ]
        assert record.access_token not in json.dumps(public_event_fields)
    engine.dispose()


def test_pending_revocation_blocks_token_issuance(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    record = store.save_token_after_issuance(
        access_token="pending-revocation-token",
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
        minimum_issue_interval_seconds=60,
    )
    assert asyncio.run(
        store.begin_revocation_if_matches(
            access_token=record.access_token,
            generation=record.generation,
        )
    )
    issue_calls = 0

    def issue_token() -> BrokerTokenGrant:
        nonlocal issue_calls
        issue_calls += 1
        return BrokerTokenGrant(
            access_token="must-not-be-issued",
            token_type="Bearer",
            access_token_expires_at=now + timedelta(hours=2),
        )

    with pytest.raises(BrokerAdapterError, match="revocation is pending"):
        issue_shared_kis_token(
            settings=settings,
            issue_token=issue_token,
            force=True,
        )

    assert issue_calls == 0
    assert isinstance(store.load_cache_state(), CachedKISTokenPendingRevocation)


def test_expired_pending_revocation_is_tombstoned_before_issuance_restarts(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    issued_at = now - timedelta(hours=2)
    expired_record = store.save_token_after_issuance(
        access_token="expired-pending-plaintext-token",
        token_type="Bearer",
        issued_at=issued_at,
        expires_at=now - timedelta(hours=1),
        refresh_after=issued_at + timedelta(minutes=30),
        minimum_issue_interval_seconds=60,
    )
    assert asyncio.run(
        store.begin_revocation_if_matches(
            access_token=expired_record.access_token,
            generation=expired_record.generation,
        )
    ) == "acquired"

    issue_calls = 0

    def issue_token() -> BrokerTokenGrant:
        nonlocal issue_calls
        issue_calls += 1
        state_during_issue = store.load_cache_state()
        assert isinstance(state_during_issue, CachedKISTokenRevocation)
        assert expired_record.access_token not in store.token_path.read_text(
            encoding="utf-8"
        )
        return BrokerTokenGrant(
            access_token="replacement-after-expired-pending",
            token_type="Bearer",
            access_token_expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    grant = issue_shared_kis_token(
        settings=settings,
        issue_token=issue_token,
        force=True,
    )

    assert issue_calls == 1
    assert grant.access_token == "replacement-after-expired-pending"
    state = store.load_cache_state()
    assert isinstance(state, CachedKISToken)
    assert state.access_token == grant.access_token
    assert expired_record.access_token not in store.token_path.read_text(encoding="utf-8")


def test_revocation_claim_is_exclusive_until_lease_expiry_then_reclaimable(
    tmp_path: Path,
) -> None:
    store = build_kis_token_cache_store(_settings(tmp_path))
    claimed_at = datetime.now(UTC)
    record = store.save_token(
        access_token="lease-owned-revocation-token",
        token_type="Bearer",
        issued_at=claimed_at,
        expires_at=claimed_at + timedelta(hours=1),
        refresh_after=claimed_at + timedelta(minutes=30),
    )

    assert asyncio.run(
        store.begin_revocation_if_matches(
            access_token=record.access_token,
            generation=record.generation,
            now=claimed_at,
        )
    ) == "acquired"
    assert asyncio.run(
        store.begin_revocation_if_matches(
            access_token=record.access_token,
            generation=record.generation,
            now=claimed_at + timedelta(seconds=store.issuance_lease_seconds - 1),
        )
    ) == "in_progress"
    pending = store.load_cache_state(
        now=claimed_at + timedelta(seconds=store.issuance_lease_seconds - 1)
    )
    assert isinstance(pending, CachedKISTokenPendingRevocation)
    assert pending.revocation_started_at == claimed_at

    reclaimed_at = claimed_at + timedelta(seconds=store.issuance_lease_seconds + 1)
    assert asyncio.run(
        store.begin_revocation_if_matches(
            access_token=record.access_token,
            generation=record.generation,
            now=reclaimed_at,
        )
    ) == "acquired"
    reclaimed = store.load_cache_state(now=reclaimed_at)
    assert isinstance(reclaimed, CachedKISTokenPendingRevocation)
    assert reclaimed.revocation_started_at == reclaimed_at
    assert asyncio.run(
        store.begin_revocation_if_matches(
            access_token="different-authoritative-token",
            generation=record.generation,
            now=reclaimed_at,
        )
    ) == "mismatch"


def test_stale_revocation_claim_cannot_finalize_a_reclaimed_lease(
    tmp_path: Path,
) -> None:
    store = build_kis_token_cache_store(_settings(tmp_path))
    reclaimed_at = datetime.now(UTC)
    first_claimed_at = reclaimed_at - timedelta(
        seconds=store.issuance_lease_seconds + 1
    )
    record = store.save_token(
        access_token="aba-safe-revocation-token",
        token_type="Bearer",
        issued_at=first_claimed_at - timedelta(minutes=1),
        expires_at=reclaimed_at + timedelta(hours=1),
        refresh_after=reclaimed_at + timedelta(minutes=30),
    )
    first_claim_id = "00000000-0000-0000-0000-0000000000a1"
    second_claim_id = "00000000-0000-0000-0000-0000000000b2"

    assert asyncio.run(
        store.begin_revocation_if_matches(
            access_token=record.access_token,
            generation=record.generation,
            claim_id=first_claim_id,
            now=first_claimed_at,
        )
    ) == "acquired"
    assert asyncio.run(
        store.begin_revocation_if_matches(
            access_token=record.access_token,
            generation=record.generation,
            claim_id=second_claim_id,
            now=reclaimed_at,
        )
    ) == "acquired"

    assert asyncio.run(
        store.renew_revocation_if_matches(
            access_token=record.access_token,
            generation=record.generation,
            claim_id=first_claim_id,
            now=reclaimed_at,
        )
    ) is None
    renewed_until = asyncio.run(
        store.renew_revocation_if_matches(
            access_token=record.access_token,
            generation=record.generation,
            claim_id=second_claim_id,
            now=reclaimed_at + timedelta(seconds=1),
        )
    )
    assert renewed_until == reclaimed_at + timedelta(
        seconds=store.issuance_lease_seconds + 1
    )

    assert asyncio.run(
        store.finalize_revocation_if_matches(
            access_token=record.access_token,
            generation=record.generation,
            claim_id=first_claim_id,
            minimum_issue_interval_seconds=60,
        )
    ) is False
    pending = store.load_cache_state(now=reclaimed_at)
    assert isinstance(pending, CachedKISTokenPendingRevocation)
    assert pending.claim_id == second_claim_id
    assert asyncio.run(
        store.finalize_revocation_if_matches(
            access_token=record.access_token,
            generation=record.generation,
            claim_id=second_claim_id,
            minimum_issue_interval_seconds=60,
        )
    ) is True


def test_pending_revocation_lease_survives_rolling_configuration_change(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    first_store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    record = first_store.save_token(
        access_token="rolling-config-revocation-token",
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
    )
    assert asyncio.run(
        first_store.begin_revocation_if_matches(
            access_token=record.access_token,
            generation=record.generation,
            now=now,
        )
    ) == "acquired"

    rolling_store = type(first_store)(
        directory=str(first_store.directory),
        environment=first_store.environment,
        app_key=settings.kis_app_key or "",
        app_secret=settings.kis_app_secret or "",
        minimum_issue_interval_seconds=60,
        issuance_lease_seconds=90,
    )
    pending = rolling_store.load_cache_state(now=now)

    assert isinstance(pending, CachedKISTokenPendingRevocation)
    assert pending.lease_expires_at == now + timedelta(seconds=75)


def test_active_revocation_claim_suppresses_a_duplicate_remote_revoke(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    record = store.save_token_after_issuance(
        access_token="single-remote-revocation-token",
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
        minimum_issue_interval_seconds=60,
    )
    assert asyncio.run(
        store.begin_revocation_if_matches(
            access_token=record.access_token,
            generation=record.generation,
        )
    ) == "acquired"
    client = QueueHttpClient(_response({"message": "must not be called"}))
    adapter = KisBrokerAdapter(settings=settings, http_client=client)
    engine, session_factory = _database(settings)

    with session_factory() as db:
        db.add(
            BrokerSession(
                id="kis:paper",
                provider="kis",
                mode="paper",
                access_token=record.access_token,
                access_token_expires_at=record.expires_at,
                generation=record.generation,
                refresh_after=record.refresh_after,
            )
        )
        db.commit()
        result = revoke_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            command=BrokerSessionCommand(),
        )

    assert result.status_code == 502
    assert result.payload["outcome"] == "rejected"
    assert client.calls == []
    assert isinstance(store.load_cache_state(), CachedKISTokenPendingRevocation)
    engine.dispose()


def test_revoke_never_calls_remote_after_claim_renewal_is_lost(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    record = store.save_token_after_issuance(
        access_token="lost-renewal-revocation-token",
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
        minimum_issue_interval_seconds=60,
    )
    client = QueueHttpClient(_response({"message": "must not be called"}))
    adapter = KisBrokerAdapter(settings=settings, http_client=client)
    engine, session_factory = _database(settings)
    monkeypatch.setattr(
        broker_auth_service,
        "renew_shared_kis_token_revocation",
        lambda **_: False,
    )
    with session_factory() as db:
        db.add(
            BrokerSession(
                id="kis:paper",
                provider="kis",
                mode="paper",
                access_token=record.access_token,
                access_token_expires_at=record.expires_at,
                generation=record.generation,
                refresh_after=record.refresh_after,
            )
        )
        db.commit()

        result = revoke_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            command=BrokerSessionCommand(),
        )

    assert result.status_code == 502
    assert result.payload["outcome"] == "rejected"
    assert client.calls == []
    assert isinstance(store.load_cache_state(), CachedKISTokenPendingRevocation)
    engine.dispose()


def test_concurrent_completed_revoke_wins_over_a_late_remote_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    record = store.save_token_after_issuance(
        access_token="concurrent-revocation-token",
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
        minimum_issue_interval_seconds=60,
    )
    adapter = KisBrokerAdapter(settings=settings, http_client=QueueHttpClient())
    engine, session_factory = _database(settings)

    def revoke_after_other_caller_completed(access_token: str) -> BrokerTokenRevocation:
        assert access_token == record.access_token
        pending = store.load_cache_state()
        assert isinstance(pending, CachedKISTokenPendingRevocation)
        assert asyncio.run(
            store.finalize_revocation_if_matches(
                access_token=record.access_token,
                generation=record.generation,
                claim_id=pending.claim_id,
                minimum_issue_interval_seconds=60,
            )
        )
        raise BrokerAdapterError("late remote revoke error", status_code=503)

    monkeypatch.setattr(adapter, "revoke", revoke_after_other_caller_completed)

    with session_factory() as db:
        db.add(
            BrokerSession(
                id="kis:paper",
                provider="kis",
                mode="paper",
                access_token=record.access_token,
                access_token_expires_at=record.expires_at,
                generation=record.generation,
                refresh_after=record.refresh_after,
            )
        )
        db.commit()
        result = revoke_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            command=BrokerSessionCommand(),
        )

        state = db.get(BrokerSession, "kis:paper")
        assert result.status_code == 200
        assert result.payload["outcome"] == "applied"
        assert state is not None
        assert state.access_token is None
        assert state.last_revoked_at is not None

    assert isinstance(store.load_cache_state(), CachedKISTokenRevocation)
    engine.dispose()


def test_pending_revocation_cas_never_replaces_a_newer_generation(
    tmp_path: Path,
) -> None:
    store = build_kis_token_cache_store(_settings(tmp_path))
    now = datetime.now(UTC)
    old_record = store.save_token(
        access_token="old-revocation-generation",
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
    )
    assert asyncio.run(
        store.begin_revocation_if_matches(
            access_token=old_record.access_token,
            generation=old_record.generation,
        )
    )
    newer_record = store.save_token(
        access_token="newer-revocation-generation",
        token_type="Bearer",
        issued_at=now + timedelta(seconds=1),
        expires_at=now + timedelta(hours=2),
        refresh_after=now + timedelta(hours=1),
    )

    assert not asyncio.run(
        store.finalize_revocation_if_matches(
            access_token=old_record.access_token,
            generation=old_record.generation,
            minimum_issue_interval_seconds=60,
        )
    )
    assert store.load_cache_state() == newer_record


def test_delayed_revoke_cannot_clear_a_newer_token_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    adapter = KisBrokerAdapter(settings=settings, http_client=QueueHttpClient())
    engine, session_factory = _database(settings)
    now = datetime.now(UTC)
    revoked_grant = BrokerTokenGrant(
        access_token="revoked-generation-token",
        token_type="Bearer",
        access_token_expires_at=now + timedelta(hours=1),
        generation="00000000-0000-0000-0000-000000000001",
        refresh_after=now + timedelta(minutes=30),
    )
    replacement_grant = BrokerTokenGrant(
        access_token="replacement-generation-token",
        token_type="Bearer",
        access_token_expires_at=now + timedelta(hours=2),
        generation="00000000-0000-0000-0000-000000000002",
        refresh_after=now + timedelta(minutes=90),
    )
    loaded_grants = iter(
        (
            revoked_grant,
            replacement_grant,
            replacement_grant,
            replacement_grant,
        )
    )
    monkeypatch.setattr(
        broker_auth_service,
        "load_shared_kis_token_for_revocation",
        lambda _settings: next(loaded_grants),
    )
    monkeypatch.setattr(
        broker_auth_service,
        "begin_shared_kis_token_revocation",
        lambda **_: SharedKISTokenRevocationClaim(
            outcome=KISTokenRevocationClaimResult.ACQUIRED,
            claim_id="00000000-0000-0000-0000-0000000000c3",
        ),
    )
    monkeypatch.setattr(
        broker_auth_service,
        "finalize_shared_kis_token_revocation",
        lambda **_: False,
    )
    monkeypatch.setattr(
        broker_auth_service,
        "renew_shared_kis_token_revocation",
        lambda **_: True,
    )
    monkeypatch.setattr(
        adapter,
        "revoke",
        lambda _token: BrokerTokenRevocation(message="revoked"),
    )
    lock_calls = 0

    def lock_newer_generation(*, db, state):
        nonlocal lock_calls
        del db
        lock_calls += 1
        state.access_token = replacement_grant.access_token
        state.token_type = replacement_grant.token_type
        state.access_token_expires_at = replacement_grant.access_token_expires_at
        state.generation = replacement_grant.generation
        state.refresh_after = replacement_grant.refresh_after
        return state

    monkeypatch.setattr(
        broker_auth_service,
        "_lock_session_for_update",
        lock_newer_generation,
    )

    with session_factory() as db:
        db.add(
            BrokerSession(
                id="kis:paper",
                provider="kis",
                mode="paper",
                api_base_url=adapter.describe().api_base_url,
                access_token=revoked_grant.access_token,
                token_type=revoked_grant.token_type,
                access_token_expires_at=revoked_grant.access_token_expires_at,
                generation=revoked_grant.generation,
                refresh_after=revoked_grant.refresh_after,
            )
        )
        db.commit()

        result = revoke_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            command=BrokerSessionCommand(),
        )

        state = db.get(BrokerSession, "kis:paper")
        assert result.status_code == 200
        assert lock_calls == 2
        assert state is not None
        assert state.generation == replacement_grant.generation
        assert state.access_token == replacement_grant.access_token
    engine.dispose()


def test_explicit_revoke_uses_shared_token_when_database_mirror_is_missing(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    record = store.save_token_after_issuance(
        access_token="shared-only-token",
        token_type="Bearer",
        issued_at=now,
        expires_at=now + timedelta(hours=1),
        refresh_after=now + timedelta(minutes=30),
        minimum_issue_interval_seconds=60,
    )
    client = QueueHttpClient(_response({"message": "token revoked"}))
    adapter = KisBrokerAdapter(settings=settings, http_client=client)
    engine, session_factory = _database(settings)

    with session_factory() as db:
        result = revoke_broker_session(
            settings=settings,
            db=db,
            adapter=adapter,
            command=BrokerSessionCommand(),
        )

    assert result.status_code == 200
    assert result.payload["outcome"] == "applied"
    assert isinstance(store.load_cache_state(), CachedKISTokenRevocation)
    assert client.calls[0]["payload"]["token"] == record.access_token
    engine.dispose()
