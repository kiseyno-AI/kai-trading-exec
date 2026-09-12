from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import time
from typing import Callable, Iterator
import uuid

from app.broker.base import (
    BrokerAdapterError,
    BrokerErrorClassification,
    BrokerTokenGrant,
    NormalizedBrokerOrderStatus,
)
from app.broker.kis_token_cache import (
    CachedKISToken,
    CachedKISTokenFailure,
    CachedKISTokenPendingRevocation,
    CachedKISTokenRevocation,
    KISTokenCacheError,
    KISTokenCacheStore,
    KISTokenCacheTimeout,
    KISTokenRevocationClaimResult,
    is_valid_bearer_token,
)
from app.core.config import Settings


@dataclass(frozen=True)
class SharedKISTokenStatus:
    status: str
    reason: str | None
    access_token_present: bool
    generation: str | None = None
    expires_at: datetime | None = None
    refresh_after: datetime | None = None


@dataclass(frozen=True)
class SharedKISTokenRevocationClaim:
    outcome: KISTokenRevocationClaimResult
    claim_id: str | None


def build_kis_token_cache_store(settings: Settings) -> KISTokenCacheStore:
    if settings.runtime_mode == "mock":
        raise BrokerAdapterError(
            "KIS shared token cache is unavailable in mock runtime",
            status_code=409,
            classification=BrokerErrorClassification.VALIDATION,
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
        )
    if not settings.kis_app_key or not settings.kis_app_secret:
        raise BrokerAdapterError(
            "KIS shared token cache credentials are unavailable",
            status_code=409,
            classification=BrokerErrorClassification.VALIDATION,
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
        )
    try:
        return KISTokenCacheStore(
            directory=settings.kis_token_cache_directory,
            environment="real" if settings.runtime_mode == "live" else "demo",
            app_key=settings.kis_app_key,
            app_secret=settings.kis_app_secret,
            minimum_issue_interval_seconds=(
                settings.kis_token_min_issue_interval_seconds
            ),
            issuance_lease_seconds=settings.kis_token_issuance_lease_seconds,
        )
    except KISTokenCacheError as exc:
        raise _cache_error() from exc


def issue_shared_kis_token(
    *,
    settings: Settings,
    issue_token: Callable[[], BrokerTokenGrant],
    force: bool = False,
) -> BrokerTokenGrant:
    store = build_kis_token_cache_store(settings)
    try:
        observed_state = store.load_cache_state() if force else None
        observed_generation = (
            observed_state.generation
            if isinstance(observed_state, CachedKISToken)
            else None
        )
        with _store_lock(store):
            now = datetime.now(UTC)
            current_state = store.load_cache_state(now=now)
            if isinstance(current_state, CachedKISTokenPendingRevocation):
                expired_pending = current_state
                if now < expired_pending.expires_at:
                    raise _retryable_cache_error(
                        "KIS token issue suppressed while shared token revocation is pending"
                    )
                resolved = store.tombstone_expired_pending_revocation_if_matches(
                    generation=expired_pending.generation,
                    access_token=expired_pending.access_token,
                    revocation_started_at=expired_pending.revocation_started_at,
                    claim_id=expired_pending.claim_id,
                    now=now,
                    minimum_issue_interval_seconds=(
                        settings.kis_token_min_issue_interval_seconds
                    ),
                )
                current_state = store.load_cache_state(now=now)
                if not resolved or isinstance(
                    current_state,
                    CachedKISTokenPendingRevocation,
                ):
                    raise _retryable_cache_error(
                        "KIS token issue suppressed while shared token revocation is pending"
                    )
            guard = store.load_failure(now=now, include_expired=True)
            disk_record = (
                current_state if isinstance(current_state, CachedKISToken) else None
            )
            success_generation_mismatch = bool(
                guard is not None
                and guard.record_type == "issuance_success"
                and (
                    (
                        disk_record is None
                        and not isinstance(current_state, CachedKISTokenRevocation)
                    )
                    or (
                        disk_record is not None
                        and guard.generation != disk_record.generation
                    )
                )
            )
            authoritative = (
                None
                if success_generation_mismatch
                or (
                    disk_record is not None
                    and _token_invalidated_by_guard(disk_record, guard)
                )
                else disk_record
            )
            if force:
                if (
                    authoritative is not None
                    and now < authoritative.expires_at
                    and authoritative.generation != observed_generation
                ):
                    return _grant(authoritative)
            elif authoritative is not None and now < authoritative.refresh_after:
                return _grant(authoritative)

            if (
                authoritative is not None
                and now
                < authoritative.issued_at
                + timedelta(seconds=settings.kis_token_min_issue_interval_seconds)
            ):
                if now < authoritative.expires_at:
                    return _grant(authoritative)
                raise _retryable_cache_error(
                    "KIS token issue suppressed during minimum issue interval"
                )
            if (
                isinstance(current_state, CachedKISTokenRevocation)
                and now < current_state.not_before
            ):
                raise _retryable_cache_error(
                    "KIS token issue suppressed after token revocation"
                )
            _raise_during_guard_cooldown(guard=guard, now=now)

            issuance_started_at = datetime.now(UTC)
            store.save_issuance_attempt(
                started_at=issuance_started_at,
                cooldown_until=issuance_started_at
                + timedelta(seconds=settings.kis_token_issuance_lease_seconds),
                invalidates_generation=(
                    disk_record.generation if disk_record is not None else None
                ),
            )
            try:
                issued_grant = issue_token()
                record = _persist_issued_token(
                    settings=settings,
                    store=store,
                    grant=issued_grant,
                    issuance_started_at=issuance_started_at,
                )
            except BrokerAdapterError as exc:
                _save_failure(settings=settings, store=store, error=exc)
                raise
            except KISTokenCacheError:
                raise
            except Exception as exc:
                failure = BrokerAdapterError(
                    "KIS token issue failed",
                    status_code=503,
                    classification=BrokerErrorClassification.NETWORK,
                    retryable=True,
                    normalized_status=NormalizedBrokerOrderStatus.RETRY_PENDING,
                )
                _save_failure(settings=settings, store=store, error=failure)
                raise failure from exc
            return _grant(record)
    except KISTokenCacheError as exc:
        raise _cache_error() from exc


def read_shared_kis_token_status(settings: Settings) -> SharedKISTokenStatus:
    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    try:
        state = store.load_cache_state(now=now)
        guard = store.load_failure(now=now, include_expired=True)
    except KISTokenCacheError as exc:
        raise _cache_error() from exc
    if isinstance(state, CachedKISToken):
        if (
            guard is not None
            and guard.record_type == "issuance_success"
            and guard.generation != state.generation
        ) or _token_invalidated_by_guard(state, guard):
            return SharedKISTokenStatus(
                status="error",
                reason="shared KIS token is invalidated by issuance state",
                access_token_present=False,
            )
        if now >= state.expires_at:
            return SharedKISTokenStatus(
                status="expired",
                reason="shared KIS token has expired",
                access_token_present=True,
                generation=state.generation,
                expires_at=state.expires_at,
                refresh_after=state.refresh_after,
            )
        if now >= state.refresh_after:
            return SharedKISTokenStatus(
                status="expired",
                reason="shared KIS token requires refresh",
                access_token_present=True,
                generation=state.generation,
                expires_at=state.expires_at,
                refresh_after=state.refresh_after,
            )
        return SharedKISTokenStatus(
            status="active",
            reason=None,
            access_token_present=True,
            generation=state.generation,
            expires_at=state.expires_at,
            refresh_after=state.refresh_after,
        )
    if isinstance(state, CachedKISTokenRevocation):
        return SharedKISTokenStatus(
            status="revoked",
            reason="shared KIS token was revoked",
            access_token_present=False,
        )
    if isinstance(state, CachedKISTokenPendingRevocation):
        return SharedKISTokenStatus(
            status="error",
            reason="shared KIS token is locally fenced; remote revocation is pending",
            access_token_present=False,
        )
    if guard is not None and now < guard.cooldown_until:
        return SharedKISTokenStatus(
            status="error",
            reason="shared KIS token issuance is unresolved or cooling down",
            access_token_present=False,
        )
    return SharedKISTokenStatus(
        status="uninitialized",
        reason="no authoritative shared KIS token is stored",
        access_token_present=False,
    )


def load_shared_kis_token_for_revocation(
    settings: Settings,
) -> BrokerTokenGrant | None:
    """Load the authoritative token without issuing or falling back to a mirror."""

    store = build_kis_token_cache_store(settings)
    now = datetime.now(UTC)
    try:
        state = store.load_cache_state(now=now)
        guard = store.load_failure(now=now, include_expired=True)
    except KISTokenCacheError as exc:
        raise _cache_error() from exc
    if isinstance(state, CachedKISTokenPendingRevocation):
        return _revocation_grant(state)
    if isinstance(state, CachedKISToken):
        if (
            guard is not None
            and guard.record_type == "issuance_success"
            and guard.generation != state.generation
        ) or _token_invalidated_by_guard(state, guard):
            raise _cache_error("KIS shared token is not authoritative")
        return _grant(state)
    if isinstance(state, CachedKISTokenRevocation):
        return None
    if guard is not None and now < guard.cooldown_until:
        raise _retryable_cache_error(
            "KIS token revocation deferred during shared issuance state"
        )
    return None


def begin_shared_kis_token_revocation(
    *,
    settings: Settings,
    access_token: str,
    generation: str,
) -> SharedKISTokenRevocationClaim:
    store = build_kis_token_cache_store(settings)
    claim_id = str(uuid.uuid4())
    try:
        outcome = store.begin_revocation_if_matches_sync(
            generation=generation,
            access_token=access_token,
            claim_id=claim_id,
        )
        return SharedKISTokenRevocationClaim(
            outcome=outcome,
            claim_id=(
                claim_id
                if outcome == KISTokenRevocationClaimResult.ACQUIRED
                else None
            ),
        )
    except KISTokenCacheTimeout as exc:
        raise _retryable_cache_error(
            "KIS shared token cache revocation claim timed out before lock acquisition",
        ) from exc
    except KISTokenCacheError as exc:
        raise _cache_error(
            "KIS shared token cache could not begin revocation"
        ) from exc


def finalize_shared_kis_token_revocation(
    *,
    settings: Settings,
    access_token: str,
    generation: str,
    claim_id: str,
) -> bool:
    store = build_kis_token_cache_store(settings)
    try:
        return store.finalize_revocation_if_matches_sync(
            generation=generation,
            access_token=access_token,
            claim_id=claim_id,
            minimum_issue_interval_seconds=(
                settings.kis_token_min_issue_interval_seconds
            ),
        )
    except KISTokenCacheTimeout as exc:
        raise _retryable_cache_error(
            "KIS shared token cache revocation finalize timed out before lock acquisition",
        ) from exc
    except KISTokenCacheError as exc:
        raise _cache_error(
            "KIS shared token cache could not finalize revocation"
        ) from exc


def renew_shared_kis_token_revocation(
    *,
    settings: Settings,
    access_token: str,
    generation: str,
    claim_id: str,
) -> bool:
    store = build_kis_token_cache_store(settings)
    try:
        return store.renew_revocation_if_matches_sync(
            generation=generation,
            access_token=access_token,
            claim_id=claim_id,
        ) is not None
    except KISTokenCacheTimeout as exc:
        raise _retryable_cache_error(
            "KIS shared token cache revocation renewal timed out before lock acquisition",
        ) from exc
    except KISTokenCacheError as exc:
        raise _cache_error(
            "KIS shared token cache could not renew revocation"
        ) from exc


def shared_kis_token_revocation_is_finalized(
    *,
    settings: Settings,
    access_token: str,
    generation: str,
) -> bool:
    store = build_kis_token_cache_store(settings)
    try:
        return store.revocation_matches(
            generation=generation,
            access_token=access_token,
        )
    except KISTokenCacheError as exc:
        raise _cache_error(
            "KIS shared token revocation state could not be verified"
        ) from exc


def invalidate_shared_kis_token(
    *,
    settings: Settings,
    access_token: str,
    generation: str | None = None,
) -> bool:
    store = build_kis_token_cache_store(settings)
    try:
        return store.invalidate_if_matches_sync(
            generation=generation,
            access_token=access_token,
            minimum_issue_interval_seconds=(
                settings.kis_token_min_issue_interval_seconds
            ),
        )
    except KISTokenCacheTimeout as exc:
        raise _retryable_cache_error(
            "KIS shared token cache invalidation timed out before lock acquisition",
        ) from exc
    except KISTokenCacheError as exc:
        raise _cache_error("KIS shared token cache could not be invalidated") from exc


@contextmanager
def _store_lock(store: KISTokenCacheStore) -> Iterator[None]:
    descriptor = store.open_lock()
    deadline = time.monotonic() + store.issuance_lease_seconds + 1
    try:
        while not store.try_acquire_lock(descriptor):
            if time.monotonic() >= deadline:
                raise _retryable_cache_error("KIS shared token cache lock timed out")
            time.sleep(0.05)
        yield
    finally:
        store.release_lock(descriptor)


def _persist_issued_token(
    *,
    settings: Settings,
    store: KISTokenCacheStore,
    grant: BrokerTokenGrant,
    issuance_started_at: datetime,
) -> CachedKISToken:
    received_at = datetime.now(UTC)
    expires_at = _as_utc(grant.access_token_expires_at)
    refresh_after = expires_at - timedelta(
        seconds=settings.kis_token_refresh_margin_seconds
    )
    if (
        not is_valid_bearer_token(grant.access_token)
        or not isinstance(grant.token_type, str)
        or grant.token_type.lower() != "bearer"
        or expires_at <= received_at
        or refresh_after <= received_at
        or expires_at <= issuance_started_at
    ):
        raise BrokerAdapterError(
            "KIS token issue returned an invalid token payload",
            status_code=502,
            classification=BrokerErrorClassification.AUTH,
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
        )
    return store.save_token_after_issuance(
        access_token=grant.access_token,
        token_type="Bearer",
        issued_at=received_at,
        expires_at=expires_at,
        refresh_after=refresh_after,
        minimum_issue_interval_seconds=settings.kis_token_min_issue_interval_seconds,
    )


def _save_failure(
    *,
    settings: Settings,
    store: KISTokenCacheStore,
    error: BrokerAdapterError,
) -> None:
    failed_at = datetime.now(UTC)
    cooldown_seconds = max(
        settings.kis_token_failure_cooldown_seconds,
        settings.kis_token_min_issue_interval_seconds,
    )
    raw_status = error.response_metadata.get("status_code")
    status_code = raw_status if isinstance(raw_status, int) else error.status_code
    error_kind = {
        BrokerErrorClassification.NETWORK: "transport",
        BrokerErrorClassification.TRANSIENT: "transport",
    }.get(error.classification, "http" if status_code is not None else "protocol")
    store.save_failure(
        error_kind=error_kind,
        status_code=status_code,
        retryable=error.retryable,
        failed_at=failed_at,
        cooldown_until=failed_at + timedelta(seconds=cooldown_seconds),
    )


def _raise_during_guard_cooldown(
    *,
    guard: CachedKISTokenFailure | None,
    now: datetime,
) -> None:
    if guard is None or now >= guard.cooldown_until:
        return
    raise _retryable_cache_error(
        "KIS token issue suppressed during shared issuance cooldown"
    )


def _token_invalidated_by_guard(
    record: CachedKISToken,
    guard: CachedKISTokenFailure | None,
) -> bool:
    if guard is None or guard.record_type == "issuance_success":
        return False
    if guard.invalidates_generation is not None:
        return guard.invalidates_generation == record.generation
    return guard.failed_at >= record.issued_at


def _grant(record: CachedKISToken) -> BrokerTokenGrant:
    return BrokerTokenGrant(
        access_token=record.access_token,
        token_type=record.token_type,
        access_token_expires_at=record.expires_at,
        generation=record.generation,
        refresh_after=record.refresh_after,
    )


def _revocation_grant(
    record: CachedKISTokenPendingRevocation,
) -> BrokerTokenGrant:
    return BrokerTokenGrant(
        access_token=record.access_token,
        token_type=record.token_type,
        access_token_expires_at=record.expires_at,
        generation=record.generation,
        refresh_after=record.refresh_after,
    )


def _cache_error(message: str = "KIS shared token cache is not safe to use") -> BrokerAdapterError:
    return BrokerAdapterError(
        message,
        status_code=409,
        classification=BrokerErrorClassification.VALIDATION,
        normalized_status=NormalizedBrokerOrderStatus.REJECTED,
    )


def _retryable_cache_error(
    message: str,
    *,
    outcome_unknown: bool = False,
) -> BrokerAdapterError:
    return BrokerAdapterError(
        message,
        status_code=503,
        classification=BrokerErrorClassification.TRANSIENT,
        retryable=True,
        code=("KIS_TOKEN_CACHE_OUTCOME_UNKNOWN" if outcome_unknown else None),
        normalized_status=NormalizedBrokerOrderStatus.RETRY_PENDING,
        response_metadata=(
            {"outcome_unknown": True, "retry_required": True}
            if outcome_unknown
            else None
        ),
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise BrokerAdapterError(
            "KIS token issue returned an invalid token payload",
            status_code=502,
            classification=BrokerErrorClassification.AUTH,
            normalized_status=NormalizedBrokerOrderStatus.REJECTED,
        )
    return value.astimezone(UTC)
