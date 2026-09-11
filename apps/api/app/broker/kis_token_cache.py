from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import stat
import time
from typing import Any, AsyncIterator, Callable, Iterator, ParamSpec, TypeVar
import uuid


TOKEN_CACHE_SCHEMA_VERSION = 1
_CACHE_FILE_MAX_BYTES = 64 * 1024
_ALLOWED_FUTURE_CLOCK_SKEW = timedelta(minutes=5)
_MAX_REVOCATION_WINDOW = timedelta(seconds=3_600) + _ALLOWED_FUTURE_CLOCK_SKEW
_COMMON_RECORD_FIELDS = frozenset(
    {
        'schema_version',
        'record_type',
        'provider',
        'environment',
        'credential_fingerprint',
    }
)
_RECORD_FIELDS = {
    'token': _COMMON_RECORD_FIELDS
    | {
        'generation',
        'access_token',
        'token_type',
        'issued_at',
        'expires_at',
        'refresh_after',
    },
    'pending_revocation': _COMMON_RECORD_FIELDS
    | {
        'generation',
        'access_token',
        'token_type',
        'issued_at',
        'expires_at',
        'refresh_after',
        'revocation_started_at',
        'claim_id',
        'lease_expires_at',
    },
    'revocation': _COMMON_RECORD_FIELDS
    | {
        'revoked_generation',
        'revoked_token_fingerprint',
        'revoked_at',
        'not_before',
    },
    'failure': _COMMON_RECORD_FIELDS
    | {
        'error_kind',
        'status_code',
        'retryable',
        'invalidates_generation',
        'failed_at',
        'cooldown_until',
    },
    'issuance_attempt': _COMMON_RECORD_FIELDS
    | {
        'attempt_id',
        'invalidates_generation',
        'started_at',
        'cooldown_until',
    },
    'issuance_success': _COMMON_RECORD_FIELDS
    | {
        'generation',
        'issued_at',
        'cooldown_until',
    },
}
_P = ParamSpec('_P')
_T = TypeVar('_T')


class KISTokenCacheError(RuntimeError):
    """Raised when the configured cache location cannot be used safely."""


class KISTokenCacheTimeout(KISTokenCacheError):
    """Raised when a shared-cache operation cannot enter before its lease expires."""


class KISTokenRevocationClaimResult(StrEnum):
    ACQUIRED = 'acquired'
    IN_PROGRESS = 'in_progress'
    MISMATCH = 'mismatch'


async def run_token_cache_io(
    operation: Callable[_P, _T],
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _T:
    """Run blocking cache I/O without abandoning it while a shared lock is held."""

    task = asyncio.create_task(asyncio.to_thread(operation, *args, **kwargs))
    cancellation_requested = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            cancellation_requested = True
            if task.done():
                break
        except BaseException:
            if cancellation_requested:
                try:
                    task.result()
                except BaseException:
                    pass
                raise asyncio.CancelledError
            raise
    if cancellation_requested:
        try:
            task.result()
        except BaseException:
            pass
        raise asyncio.CancelledError
    return result


@dataclass(frozen=True)
class CachedKISToken:
    access_token: str
    token_type: str
    issued_at: datetime
    expires_at: datetime
    refresh_after: datetime
    generation: str


@dataclass(frozen=True)
class CachedKISTokenPendingRevocation:
    access_token: str
    token_type: str
    issued_at: datetime
    expires_at: datetime
    refresh_after: datetime
    generation: str
    revocation_started_at: datetime
    claim_id: str
    lease_expires_at: datetime


@dataclass(frozen=True)
class CachedKISTokenRevocation:
    revoked_generation: str
    revoked_token_fingerprint: str
    revoked_at: datetime
    not_before: datetime


@dataclass(frozen=True)
class CachedKISTokenFailure:
    record_type: str
    error_kind: str
    status_code: int | None
    retryable: bool
    failed_at: datetime
    cooldown_until: datetime
    attempt_id: str | None = None
    invalidates_generation: str | None = None
    generation: str | None = None


class KISTokenCacheStore:
    """Permission-restricted, identity-scoped durable KIS OAuth token cache."""

    def __init__(
        self,
        *,
        directory: str,
        environment: str,
        app_key: str,
        app_secret: str,
        minimum_issue_interval_seconds: int = 60,
        issuance_lease_seconds: int = 90,
    ) -> None:
        configured_directory = str(directory).strip()
        configured_path = Path(configured_directory)
        expanded_directory = configured_path.expanduser()
        if (
            not configured_directory
            or configured_directory in {'~', '~/'}
            or not expanded_directory.is_absolute()
            or expanded_directory.parent == expanded_directory
            or expanded_directory == Path.home()
            or '..' in configured_path.parts
            or configured_path in {Path('.'), Path('..')}
        ):
            raise KISTokenCacheError(
                'KIS token cache directory must be an absolute, dedicated leaf path'
            )
        if environment not in {'real', 'demo'}:
            raise KISTokenCacheError('KIS token cache environment must be real or demo')
        if not app_key or not app_secret:
            raise KISTokenCacheError('KIS token cache credentials must not be empty')
        if (
            isinstance(minimum_issue_interval_seconds, bool)
            or not isinstance(minimum_issue_interval_seconds, int)
            or not 60 <= minimum_issue_interval_seconds <= 3600
            or isinstance(issuance_lease_seconds, bool)
            or not isinstance(issuance_lease_seconds, int)
            or not minimum_issue_interval_seconds <= issuance_lease_seconds <= 3600
        ):
            raise KISTokenCacheError('KIS token cache timing contract is invalid')
        self.directory = expanded_directory
        self.environment = environment
        self.minimum_issue_interval_seconds = minimum_issue_interval_seconds
        self.issuance_lease_seconds = issuance_lease_seconds
        fingerprint = hmac.new(
            app_secret.encode('utf-8'),
            f'kis-token-cache-v1\0{environment}\0{app_key}'.encode('utf-8'),
            hashlib.sha256,
        ).hexdigest()
        self.credential_fingerprint = f'hmac-sha256:{fingerprint}'
        self.cache_key = f'{environment}:{self.credential_fingerprint}'
        file_key = f'{environment}-{fingerprint[:32]}'
        self.token_path = self.directory / f'{file_key}.token.json'
        self.lock_path = self.directory / f'{file_key}.lock'
        self.failure_path = self.directory / f'{file_key}.failure.json'

    def open_lock(self) -> int:
        directory_descriptor = self._open_cache_directory(create=True)
        if directory_descriptor is None:
            raise KISTokenCacheError('KIS token cache directory could not be created')
        no_follow = getattr(os, 'O_NOFOLLOW', 0)
        created = False
        descriptor: int | None = None
        try:
            try:
                descriptor = os.open(
                    self.lock_path.name,
                    os.O_CREAT | os.O_EXCL | os.O_RDWR | no_follow,
                    0o600,
                    dir_fd=directory_descriptor,
                )
                created = True
            except FileExistsError:
                descriptor = os.open(
                    self.lock_path.name,
                    os.O_RDWR | no_follow,
                    dir_fd=directory_descriptor,
                )
            lock_stat = os.fstat(descriptor)
            if created:
                os.fchmod(descriptor, 0o600)
                lock_stat = os.fstat(descriptor)
            if not self._is_safe_private_file(lock_stat):
                raise KISTokenCacheError('KIS token cache lock is not a private regular file')
        except BaseException as exc:
            if descriptor is not None:
                os.close(descriptor)
            if isinstance(exc, KISTokenCacheError):
                raise
            raise KISTokenCacheError('KIS token cache lock is not safe to use') from exc
        finally:
            os.close(directory_descriptor)
        if descriptor is None:
            raise KISTokenCacheError('KIS token cache lock was not opened')
        return descriptor

    @staticmethod
    def try_acquire_lock(descriptor: int) -> bool:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True

    @asynccontextmanager
    async def lock(self, *, poll_interval_seconds: float = 0.05) -> AsyncIterator[None]:
        open_task = asyncio.create_task(asyncio.to_thread(self.open_lock))
        cancellation_requested = False
        while True:
            try:
                descriptor = await asyncio.shield(open_task)
                break
            except asyncio.CancelledError:
                cancellation_requested = True
                if open_task.done():
                    break
            except BaseException:
                if cancellation_requested:
                    try:
                        open_task.result()
                    except BaseException:
                        pass
                    raise asyncio.CancelledError
                raise
        if cancellation_requested:
            try:
                descriptor = open_task.result()
            except BaseException:
                pass
            else:
                self.release_lock(descriptor)
            raise asyncio.CancelledError
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.issuance_lease_seconds
        try:
            while not self.try_acquire_lock(descriptor):
                remaining_seconds = deadline - loop.time()
                if remaining_seconds <= 0:
                    raise KISTokenCacheTimeout(
                        'KIS shared token cache lock timed out'
                    )
                await asyncio.sleep(min(poll_interval_seconds, remaining_seconds))
            yield
        finally:
            self.release_lock(descriptor)

    @contextmanager
    def sync_lock(
        self,
        *,
        poll_interval_seconds: float = 0.05,
    ) -> Iterator[None]:
        """Acquire the cache lock synchronously with a bounded admission wait."""

        deadline = time.monotonic() + self.issuance_lease_seconds
        descriptor = self.open_lock()
        try:
            while True:
                remaining_seconds = deadline - time.monotonic()
                if remaining_seconds <= 0:
                    raise KISTokenCacheTimeout(
                        'KIS shared token cache lock timed out'
                    )
                if self.try_acquire_lock(descriptor):
                    break
                time.sleep(min(poll_interval_seconds, remaining_seconds))
            yield
        finally:
            self.release_lock(descriptor)

    @staticmethod
    def release_lock(descriptor: int) -> None:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def load_token(self, *, now: datetime | None = None) -> CachedKISToken | None:
        return self.load_token_record(now=now, require_reusable=True)

    def load_cache_state(
        self,
        *,
        now: datetime | None = None,
    ) -> (
        CachedKISToken
        | CachedKISTokenPendingRevocation
        | CachedKISTokenRevocation
        | None
    ):
        payload = self._read_json(self.token_path)
        if payload is None:
            return None
        self._validate_common_record(payload)
        current_time = now or datetime.now(timezone.utc)
        record_type = payload.get('record_type')
        self._validate_record_fields(payload, record_type)
        if record_type == 'token':
            return self._parse_token_record(payload, now=current_time)
        if record_type == 'pending_revocation':
            return self._parse_pending_revocation_record(payload, now=current_time)
        if record_type == 'revocation':
            return self._parse_revocation_record(payload, now=current_time)
        raise KISTokenCacheError('KIS token cache record type is invalid')

    def load_token_record(
        self,
        *,
        now: datetime | None = None,
        require_reusable: bool = False,
    ) -> CachedKISToken | None:
        current_time = now or datetime.now(timezone.utc)
        state = self.load_cache_state(now=current_time)
        if not isinstance(state, CachedKISToken):
            return None
        if require_reusable and current_time >= state.refresh_after:
            return None
        return state

    def save_token(
        self,
        *,
        access_token: str,
        token_type: str,
        issued_at: datetime,
        expires_at: datetime,
        refresh_after: datetime,
    ) -> CachedKISToken:
        record = self._new_token_record(
            access_token=access_token,
            token_type=token_type,
            issued_at=issued_at,
            expires_at=expires_at,
            refresh_after=refresh_after,
        )
        self._write_token(record)
        return record

    def save_token_after_issuance(
        self,
        *,
        access_token: str,
        token_type: str,
        issued_at: datetime,
        expires_at: datetime,
        refresh_after: datetime,
        minimum_issue_interval_seconds: int,
    ) -> CachedKISToken:
        if (
            isinstance(minimum_issue_interval_seconds, bool)
            or minimum_issue_interval_seconds != self.minimum_issue_interval_seconds
        ):
            raise KISTokenCacheError('KIS token issuance interval does not match cache contract')
        record = self._new_token_record(
            access_token=access_token,
            token_type=token_type,
            issued_at=issued_at,
            expires_at=expires_at,
            refresh_after=refresh_after,
        )
        cooldown_until = record.issued_at + timedelta(
            seconds=self.minimum_issue_interval_seconds
        )
        self._write_token(record)
        self._write_json(
            self.failure_path,
            {
                'schema_version': TOKEN_CACHE_SCHEMA_VERSION,
                'record_type': 'issuance_success',
                'provider': 'kis',
                'environment': self.environment,
                'credential_fingerprint': self.credential_fingerprint,
                'generation': record.generation,
                'issued_at': record.issued_at.isoformat(),
                'cooldown_until': cooldown_until.isoformat(),
            },
        )
        return record

    def invalidate_if_matches_sync(
        self,
        *,
        generation: str | None = None,
        access_token: str | None = None,
        minimum_issue_interval_seconds: int = 60,
    ) -> bool:
        if generation is None and access_token is None:
            raise ValueError('generation or access_token is required for CAS invalidation')
        if (
            isinstance(minimum_issue_interval_seconds, bool)
            or minimum_issue_interval_seconds != self.minimum_issue_interval_seconds
        ):
            raise KISTokenCacheError('KIS token issuance interval does not match cache contract')
        with self.sync_lock():
            state = self.load_cache_state()
            if not isinstance(state, CachedKISToken):
                return False
            if access_token is not None:
                if not hmac.compare_digest(state.access_token, access_token):
                    return False
            if generation is not None and not hmac.compare_digest(
                state.generation,
                generation,
            ):
                return False
            self._write_revocation_tombstone(
                state=state,
                revoked_at=datetime.now(timezone.utc),
            )
            return True

    async def invalidate_if_matches(
        self,
        *,
        generation: str | None = None,
        access_token: str | None = None,
        minimum_issue_interval_seconds: int = 60,
    ) -> bool:
        return await run_token_cache_io(
            self.invalidate_if_matches_sync,
            generation=generation,
            access_token=access_token,
            minimum_issue_interval_seconds=minimum_issue_interval_seconds,
        )

    def begin_revocation_if_matches_sync(
        self,
        *,
        generation: str,
        access_token: str,
        claim_id: str | None = None,
        now: datetime | None = None,
    ) -> KISTokenRevocationClaimResult:
        """Fence a token from reuse while retaining it for remote revocation retry."""

        with self.sync_lock():
            current_time = now or datetime.now(timezone.utc)
            _require_timezone_aware(current_time)
            current_time = current_time.astimezone(timezone.utc)
            state = self.load_cache_state(now=current_time)
            if isinstance(state, CachedKISTokenPendingRevocation):
                if not hmac.compare_digest(
                    state.access_token,
                    access_token,
                ) or not hmac.compare_digest(state.generation, generation):
                    return KISTokenRevocationClaimResult.MISMATCH
                if current_time < state.lease_expires_at:
                    return KISTokenRevocationClaimResult.IN_PROGRESS
            elif not isinstance(state, CachedKISToken):
                return KISTokenRevocationClaimResult.MISMATCH
            if not hmac.compare_digest(
                state.access_token,
                access_token,
            ) or not hmac.compare_digest(state.generation, generation):
                return KISTokenRevocationClaimResult.MISMATCH
            current_claim_id = claim_id or str(uuid.uuid4())
            if not _is_uuid_text(current_claim_id):
                raise KISTokenCacheError(
                    'KIS token revocation claim identity is invalid'
                )
            lease_expires_at = current_time + timedelta(
                seconds=self.issuance_lease_seconds
            )
            self._write_json(
                self.token_path,
                {
                    'schema_version': TOKEN_CACHE_SCHEMA_VERSION,
                    'record_type': 'pending_revocation',
                    'provider': 'kis',
                    'environment': self.environment,
                    'credential_fingerprint': self.credential_fingerprint,
                    'generation': state.generation,
                    'access_token': state.access_token,
                    'token_type': state.token_type,
                    'issued_at': state.issued_at.isoformat(),
                    'expires_at': state.expires_at.isoformat(),
                    'refresh_after': state.refresh_after.isoformat(),
                    'revocation_started_at': current_time.isoformat(),
                    'claim_id': current_claim_id,
                    'lease_expires_at': lease_expires_at.isoformat(),
                },
            )
            return KISTokenRevocationClaimResult.ACQUIRED

    async def begin_revocation_if_matches(
        self,
        *,
        generation: str,
        access_token: str,
        claim_id: str | None = None,
        now: datetime | None = None,
    ) -> KISTokenRevocationClaimResult:
        return await run_token_cache_io(
            self.begin_revocation_if_matches_sync,
            generation=generation,
            access_token=access_token,
            claim_id=claim_id,
            now=now,
        )

    def finalize_revocation_if_matches_sync(
        self,
        *,
        generation: str,
        access_token: str,
        claim_id: str | None = None,
        minimum_issue_interval_seconds: int = 60,
    ) -> bool:
        """Replace a matching pending target with a tokenless tombstone."""

        if (
            isinstance(minimum_issue_interval_seconds, bool)
            or minimum_issue_interval_seconds != self.minimum_issue_interval_seconds
        ):
            raise KISTokenCacheError('KIS token issuance interval does not match cache contract')
        with self.sync_lock():
            state = self.load_cache_state()
            if not isinstance(state, CachedKISTokenPendingRevocation):
                return False
            if claim_id is None or not _is_uuid_text(claim_id):
                return False
            if not hmac.compare_digest(
                state.access_token,
                access_token,
            ) or not hmac.compare_digest(
                state.generation,
                generation,
            ) or not hmac.compare_digest(state.claim_id, claim_id):
                return False
            self._write_revocation_tombstone(
                state=state,
                revoked_at=datetime.now(timezone.utc),
            )
            return True

    async def finalize_revocation_if_matches(
        self,
        *,
        generation: str,
        access_token: str,
        claim_id: str | None = None,
        minimum_issue_interval_seconds: int = 60,
    ) -> bool:
        return await run_token_cache_io(
            self.finalize_revocation_if_matches_sync,
            generation=generation,
            access_token=access_token,
            claim_id=claim_id,
            minimum_issue_interval_seconds=minimum_issue_interval_seconds,
        )

    def renew_revocation_if_matches_sync(
        self,
        *,
        generation: str,
        access_token: str,
        claim_id: str,
        now: datetime | None = None,
    ) -> datetime | None:
        """Extend only the caller's still-live revocation lease."""

        with self.sync_lock():
            current_time = now or datetime.now(timezone.utc)
            _require_timezone_aware(current_time)
            current_time = current_time.astimezone(timezone.utc)
            state = self.load_cache_state(now=current_time)
            if (
                not isinstance(state, CachedKISTokenPendingRevocation)
                or current_time >= state.lease_expires_at
                or not hmac.compare_digest(state.access_token, access_token)
                or not hmac.compare_digest(state.generation, generation)
                or not hmac.compare_digest(state.claim_id, claim_id)
            ):
                return None
            lease_expires_at = current_time + timedelta(
                seconds=self.issuance_lease_seconds
            )
            self._write_json(
                self.token_path,
                {
                    'schema_version': TOKEN_CACHE_SCHEMA_VERSION,
                    'record_type': 'pending_revocation',
                    'provider': 'kis',
                    'environment': self.environment,
                    'credential_fingerprint': self.credential_fingerprint,
                    'generation': state.generation,
                    'access_token': state.access_token,
                    'token_type': state.token_type,
                    'issued_at': state.issued_at.isoformat(),
                    'expires_at': state.expires_at.isoformat(),
                    'refresh_after': state.refresh_after.isoformat(),
                    'revocation_started_at': current_time.isoformat(),
                    'claim_id': state.claim_id,
                    'lease_expires_at': lease_expires_at.isoformat(),
                },
            )
            return lease_expires_at

    async def renew_revocation_if_matches(
        self,
        *,
        generation: str,
        access_token: str,
        claim_id: str,
        now: datetime | None = None,
    ) -> datetime | None:
        return await run_token_cache_io(
            self.renew_revocation_if_matches_sync,
            generation=generation,
            access_token=access_token,
            claim_id=claim_id,
            now=now,
        )

    def tombstone_expired_pending_revocation_if_matches(
        self,
        *,
        generation: str,
        access_token: str,
        revocation_started_at: datetime,
        claim_id: str,
        now: datetime,
        minimum_issue_interval_seconds: int = 60,
    ) -> bool:
        """CAS an expired pending token to a tombstone while the caller holds the lock."""

        _require_timezone_aware(revocation_started_at, now)
        if (
            isinstance(minimum_issue_interval_seconds, bool)
            or minimum_issue_interval_seconds != self.minimum_issue_interval_seconds
        ):
            raise KISTokenCacheError('KIS token issuance interval does not match cache contract')
        current_time = now.astimezone(timezone.utc)
        state = self.load_cache_state(now=current_time)
        if not isinstance(state, CachedKISTokenPendingRevocation):
            return False
        if (
            current_time < state.expires_at
            or not hmac.compare_digest(state.access_token, access_token)
            or not hmac.compare_digest(state.generation, generation)
            or not hmac.compare_digest(state.claim_id, claim_id)
            or state.revocation_started_at
            != revocation_started_at.astimezone(timezone.utc)
        ):
            return False
        self._write_revocation_tombstone(state=state, revoked_at=current_time)
        return True

    def _write_revocation_tombstone(
        self,
        *,
        state: CachedKISToken | CachedKISTokenPendingRevocation,
        revoked_at: datetime,
    ) -> None:
        not_before = max(
            revoked_at,
            state.issued_at + timedelta(seconds=self.minimum_issue_interval_seconds),
        )
        token_digest = hashlib.sha256(state.access_token.encode('ascii')).hexdigest()
        self._write_json(
            self.token_path,
            {
                'schema_version': TOKEN_CACHE_SCHEMA_VERSION,
                'record_type': 'revocation',
                'provider': 'kis',
                'environment': self.environment,
                'credential_fingerprint': self.credential_fingerprint,
                'revoked_generation': state.generation,
                'revoked_token_fingerprint': f'sha256:{token_digest}',
                'revoked_at': revoked_at.isoformat(),
                'not_before': not_before.isoformat(),
            },
        )

    def revocation_matches(
        self,
        *,
        generation: str,
        access_token: str,
    ) -> bool:
        state = self.load_cache_state()
        if not isinstance(state, CachedKISTokenRevocation):
            return False
        token_digest = hashlib.sha256(access_token.encode('ascii')).hexdigest()
        return hmac.compare_digest(
            state.revoked_generation,
            generation,
        ) and hmac.compare_digest(
            state.revoked_token_fingerprint,
            f'sha256:{token_digest}',
        )

    def _new_token_record(
        self,
        *,
        access_token: str,
        token_type: str,
        issued_at: datetime,
        expires_at: datetime,
        refresh_after: datetime,
    ) -> CachedKISToken:
        _require_timezone_aware(issued_at, expires_at, refresh_after)
        record = CachedKISToken(
            access_token=access_token,
            token_type=token_type,
            issued_at=issued_at.astimezone(timezone.utc),
            expires_at=expires_at.astimezone(timezone.utc),
            refresh_after=refresh_after.astimezone(timezone.utc),
            generation=str(uuid.uuid4()),
        )
        if (
            not is_valid_bearer_token(record.access_token)
            or record.token_type != 'Bearer'
            or not record.issued_at <= record.refresh_after <= record.expires_at
            or record.expires_at <= record.issued_at
            or record.expires_at - record.issued_at > timedelta(seconds=172_800)
            or record.issued_at > datetime.now(timezone.utc) + timedelta(minutes=5)
        ):
            raise KISTokenCacheError('KIS token cache record is invalid')
        return record

    def _write_token(self, record: CachedKISToken) -> None:
        payload = {
            'schema_version': TOKEN_CACHE_SCHEMA_VERSION,
            'record_type': 'token',
            'provider': 'kis',
            'environment': self.environment,
            'credential_fingerprint': self.credential_fingerprint,
            'generation': record.generation,
            'access_token': record.access_token,
            'token_type': record.token_type,
            'issued_at': record.issued_at.isoformat(),
            'expires_at': record.expires_at.isoformat(),
            'refresh_after': record.refresh_after.isoformat(),
        }
        self._write_json(self.token_path, payload)

    def _validate_common_record(self, payload: dict[str, Any]) -> None:
        schema_version = payload.get('schema_version')
        if (
            not isinstance(schema_version, int)
            or isinstance(schema_version, bool)
            or schema_version != TOKEN_CACHE_SCHEMA_VERSION
            or payload.get('provider') != 'kis'
            or payload.get('environment') != self.environment
            or payload.get('credential_fingerprint') != self.credential_fingerprint
        ):
            raise KISTokenCacheError('KIS token cache identity or schema is invalid')

    @staticmethod
    def _validate_record_fields(
        payload: dict[str, Any],
        record_type: object,
    ) -> None:
        expected_fields = (
            _RECORD_FIELDS.get(record_type)
            if isinstance(record_type, str)
            else None
        )
        if expected_fields is None or set(payload) != expected_fields:
            raise KISTokenCacheError('KIS token cache record fields are invalid')

    def _parse_token_record(
        self,
        payload: dict[str, Any],
        *,
        now: datetime,
    ) -> CachedKISToken:
        access_token = payload.get('access_token')
        token_type = payload.get('token_type')
        generation = payload.get('generation')
        issued_at = _parse_utc_datetime(payload.get('issued_at'))
        expires_at = _parse_utc_datetime(payload.get('expires_at'))
        refresh_after = _parse_utc_datetime(payload.get('refresh_after'))
        if (
            not is_valid_bearer_token(access_token)
            or token_type != 'Bearer'
            or not _is_uuid_text(generation)
            or issued_at is None
            or expires_at is None
            or refresh_after is None
            or not issued_at <= refresh_after <= expires_at
            or expires_at <= issued_at
            or expires_at - issued_at > timedelta(seconds=172_800)
            or issued_at > now + timedelta(minutes=5)
        ):
            raise KISTokenCacheError('KIS token cache record is invalid')
        return CachedKISToken(
            access_token=access_token,
            token_type=token_type,
            issued_at=issued_at,
            expires_at=expires_at,
            refresh_after=refresh_after,
            generation=generation,
        )

    def _parse_revocation_record(
        self,
        payload: dict[str, Any],
        *,
        now: datetime,
    ) -> CachedKISTokenRevocation:
        revoked_generation = payload.get('revoked_generation')
        revoked_token_fingerprint = payload.get('revoked_token_fingerprint')
        revoked_at = _parse_utc_datetime(payload.get('revoked_at'))
        not_before = _parse_utc_datetime(payload.get('not_before'))
        if (
            not _is_uuid_text(revoked_generation)
            or not _is_sha256_fingerprint(revoked_token_fingerprint)
            or revoked_at is None
            or not_before is None
            or not revoked_at <= not_before <= revoked_at + _MAX_REVOCATION_WINDOW
            or revoked_at > now + _ALLOWED_FUTURE_CLOCK_SKEW
        ):
            raise KISTokenCacheError('KIS token revocation record is invalid')
        return CachedKISTokenRevocation(
            revoked_generation=revoked_generation,
            revoked_token_fingerprint=revoked_token_fingerprint,
            revoked_at=revoked_at,
            not_before=not_before,
        )

    def _parse_pending_revocation_record(
        self,
        payload: dict[str, Any],
        *,
        now: datetime,
    ) -> CachedKISTokenPendingRevocation:
        access_token = payload.get('access_token')
        token_type = payload.get('token_type')
        generation = payload.get('generation')
        issued_at = _parse_utc_datetime(payload.get('issued_at'))
        expires_at = _parse_utc_datetime(payload.get('expires_at'))
        refresh_after = _parse_utc_datetime(payload.get('refresh_after'))
        revocation_started_at = _parse_utc_datetime(
            payload.get('revocation_started_at')
        )
        claim_id = payload.get('claim_id')
        lease_expires_at = _parse_utc_datetime(payload.get('lease_expires_at'))
        if (
            not is_valid_bearer_token(access_token)
            or token_type != 'Bearer'
            or not _is_uuid_text(generation)
            or issued_at is None
            or expires_at is None
            or refresh_after is None
            or revocation_started_at is None
            or not _is_uuid_text(claim_id)
            or lease_expires_at is None
            or not issued_at <= refresh_after <= expires_at
            or expires_at <= issued_at
            or expires_at - issued_at > timedelta(seconds=172_800)
            or issued_at > revocation_started_at + _ALLOWED_FUTURE_CLOCK_SKEW
            or revocation_started_at > now + _ALLOWED_FUTURE_CLOCK_SKEW
            or not revocation_started_at
            < lease_expires_at
            <= revocation_started_at + timedelta(seconds=3600)
        ):
            raise KISTokenCacheError('KIS token pending revocation record is invalid')
        return CachedKISTokenPendingRevocation(
            access_token=access_token,
            token_type=token_type,
            issued_at=issued_at,
            expires_at=expires_at,
            refresh_after=refresh_after,
            generation=generation,
            revocation_started_at=revocation_started_at,
            claim_id=claim_id,
            lease_expires_at=lease_expires_at,
        )

    def load_failure(
        self,
        *,
        now: datetime | None = None,
        include_expired: bool = False,
    ) -> CachedKISTokenFailure | None:
        payload = self._read_json(self.failure_path)
        if payload is None:
            return None
        self._validate_common_record(payload)
        record_type = payload.get('record_type')
        if (
            not isinstance(record_type, str)
            or record_type not in {'failure', 'issuance_attempt', 'issuance_success'}
        ):
            raise KISTokenCacheError('KIS token failure record type is invalid')
        self._validate_record_fields(payload, record_type)

        error_kind = payload.get('error_kind')
        status_code_value = payload.get('status_code')
        if status_code_value is not None and (
            not isinstance(status_code_value, int)
            or isinstance(status_code_value, bool)
            or not 100 <= status_code_value <= 599
        ):
            raise KISTokenCacheError('KIS token failure status code is invalid')
        status_code = status_code_value
        retryable = payload.get('retryable')
        timestamp_field = {
            'failure': 'failed_at',
            'issuance_attempt': 'started_at',
            'issuance_success': 'issued_at',
        }[record_type]
        failed_at = _parse_utc_datetime(payload.get(timestamp_field))
        cooldown_until = _parse_utc_datetime(payload.get('cooldown_until'))
        attempt_id_value = payload.get('attempt_id')
        attempt_id = attempt_id_value if isinstance(attempt_id_value, str) else None
        invalidates_generation_value = payload.get('invalidates_generation')
        invalidates_generation = (
            invalidates_generation_value
            if isinstance(invalidates_generation_value, str)
            else None
        )
        current_time = now or datetime.now(timezone.utc)
        if failed_at is None or cooldown_until is None:
            raise KISTokenCacheError('KIS token failure timestamps are invalid')
        if record_type == 'failure' and (
            error_kind not in {'http', 'timeout', 'transport', 'protocol'}
            or not isinstance(retryable, bool)
        ):
            raise KISTokenCacheError('KIS token failure details are invalid')
        if record_type == 'issuance_attempt' and not _is_uuid_text(attempt_id):
            raise KISTokenCacheError('KIS token issuance attempt is invalid')
        if record_type in {'failure', 'issuance_attempt'} and (
            invalidates_generation_value is not None
            and not _is_uuid_text(invalidates_generation)
        ):
            raise KISTokenCacheError('KIS token issuance baseline is invalid')
        if record_type == 'issuance_success' and not _is_uuid_text(payload.get('generation')):
            raise KISTokenCacheError('KIS token issuance success record is invalid')
        generation = payload.get('generation') if record_type == 'issuance_success' else None
        cooldown_duration_seconds = (cooldown_until - failed_at).total_seconds()
        local_floor_seconds = (
            self.issuance_lease_seconds
            if record_type == 'issuance_attempt'
            else self.minimum_issue_interval_seconds
        )
        if (
            failed_at > cooldown_until
            or failed_at > current_time
            or not 0 < cooldown_duration_seconds <= 3600
        ):
            raise KISTokenCacheError('KIS token failure cooldown bounds are invalid')
        cooldown_until = max(
            cooldown_until,
            failed_at + timedelta(seconds=local_floor_seconds),
        )
        if not include_expired and current_time >= cooldown_until:
            return None
        if record_type == 'issuance_attempt':
            error_kind = 'ambiguous_attempt'
            status_code = None
            retryable = True
        elif record_type == 'issuance_success':
            error_kind = 'minimum_issue_interval'
            status_code = None
            retryable = True
        return CachedKISTokenFailure(
            record_type=record_type,
            error_kind=error_kind,
            status_code=status_code,
            retryable=retryable,
            failed_at=failed_at,
            cooldown_until=cooldown_until,
            attempt_id=attempt_id,
            invalidates_generation=invalidates_generation,
            generation=generation,
        )

    def save_issuance_attempt(
        self,
        *,
        started_at: datetime,
        cooldown_until: datetime,
        invalidates_generation: str | None,
    ) -> str:
        _require_timezone_aware(started_at, cooldown_until)
        duration_seconds = (cooldown_until - started_at).total_seconds()
        if not self.issuance_lease_seconds <= duration_seconds <= 3600:
            raise KISTokenCacheError('KIS token issuance lease does not match cache contract')
        attempt_id = str(uuid.uuid4())
        self._write_json(
            self.failure_path,
            {
                'schema_version': TOKEN_CACHE_SCHEMA_VERSION,
                'record_type': 'issuance_attempt',
                'provider': 'kis',
                'environment': self.environment,
                'credential_fingerprint': self.credential_fingerprint,
                'attempt_id': attempt_id,
                'invalidates_generation': invalidates_generation,
                'started_at': started_at.astimezone(timezone.utc).isoformat(),
                'cooldown_until': cooldown_until.astimezone(timezone.utc).isoformat(),
            },
        )
        return attempt_id

    def save_failure(
        self,
        *,
        error_kind: str,
        status_code: int | None,
        retryable: bool,
        failed_at: datetime,
        cooldown_until: datetime,
    ) -> None:
        _require_timezone_aware(failed_at, cooldown_until)
        duration_seconds = (cooldown_until - failed_at).total_seconds()
        if not self.minimum_issue_interval_seconds <= duration_seconds <= 3600:
            raise KISTokenCacheError('KIS token failure cooldown does not match cache contract')
        issuance_guard = self.load_failure(include_expired=True)
        invalidates_generation = (
            issuance_guard.invalidates_generation
            if issuance_guard is not None
            and issuance_guard.record_type == 'issuance_attempt'
            else None
        )
        self._write_json(
            self.failure_path,
            {
                'schema_version': TOKEN_CACHE_SCHEMA_VERSION,
                'record_type': 'failure',
                'provider': 'kis',
                'environment': self.environment,
                'credential_fingerprint': self.credential_fingerprint,
                'error_kind': error_kind,
                'status_code': status_code,
                'retryable': retryable,
                'invalidates_generation': invalidates_generation,
                'failed_at': failed_at.astimezone(timezone.utc).isoformat(),
                'cooldown_until': cooldown_until.astimezone(timezone.utc).isoformat(),
            },
        )

    def _open_cache_directory(self, *, create: bool) -> int | None:
        directory = self.directory
        if not directory.is_absolute():
            raise KISTokenCacheError('KIS token cache directory must resolve to an absolute path')

        flags = os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0) | getattr(os, 'O_NOFOLLOW', 0)
        current_descriptor = os.open(os.path.sep, flags)
        try:
            components = directory.parts[1:]
            for index, component in enumerate(components):
                is_final = index == len(components) - 1
                try:
                    next_descriptor = os.open(component, flags, dir_fd=current_descriptor)
                except FileNotFoundError:
                    if not create:
                        os.close(current_descriptor)
                        return None
                    try:
                        os.mkdir(component, mode=0o700, dir_fd=current_descriptor)
                        os.fsync(current_descriptor)
                    except FileExistsError:
                        pass
                    next_descriptor = os.open(component, flags, dir_fd=current_descriptor)
                component_stat = os.fstat(next_descriptor)
                if not stat.S_ISDIR(component_stat.st_mode):
                    os.close(next_descriptor)
                    raise KISTokenCacheError(
                        'KIS token cache path contains a non-directory component'
                    )
                if is_final:
                    if hasattr(os, 'getuid') and component_stat.st_uid != os.getuid():
                        os.close(next_descriptor)
                        raise KISTokenCacheError(
                            'KIS token cache directory must be owned by the current user'
                        )
                    if stat.S_IMODE(component_stat.st_mode) != 0o700:
                        os.close(next_descriptor)
                        raise KISTokenCacheError(
                            'KIS token cache directory permissions must be 0700'
                        )
                elif (
                    stat.S_IMODE(component_stat.st_mode) & 0o022
                    and not component_stat.st_mode & stat.S_ISVTX
                ):
                    os.close(next_descriptor)
                    raise KISTokenCacheError(
                        'KIS token cache path has a writable non-sticky ancestor'
                    )
                os.close(current_descriptor)
                current_descriptor = next_descriptor
            return current_descriptor
        except BaseException as exc:
            os.close(current_descriptor)
            if isinstance(exc, KISTokenCacheError):
                raise
            raise KISTokenCacheError('KIS token cache directory is not safe to use') from exc

    def _read_json(self, path: Path) -> dict[str, Any] | None:
        directory_descriptor = self._open_cache_directory(create=False)
        if directory_descriptor is None:
            return None
        descriptor: int | None = None
        try:
            try:
                descriptor = os.open(
                    path.name,
                    os.O_RDONLY
                    | getattr(os, 'O_NOFOLLOW', 0)
                    | getattr(os, 'O_NONBLOCK', 0),
                    dir_fd=directory_descriptor,
                )
            except FileNotFoundError:
                return None
            except OSError as exc:
                raise KISTokenCacheError('KIS token cache file is not safe to open') from exc

            file_stat = os.fstat(descriptor)
            if not self._is_safe_private_file(file_stat):
                raise KISTokenCacheError('KIS token cache file is not a private regular file')
            if file_stat.st_size > _CACHE_FILE_MAX_BYTES:
                raise KISTokenCacheError('KIS token cache file is unexpectedly large')

            chunks: list[bytes] = []
            bytes_read = 0
            while True:
                chunk = os.read(descriptor, min(8192, _CACHE_FILE_MAX_BYTES + 1 - bytes_read))
                if not chunk:
                    break
                chunks.append(chunk)
                bytes_read += len(chunk)
                if bytes_read > _CACHE_FILE_MAX_BYTES:
                    raise KISTokenCacheError('KIS token cache file is unexpectedly large')
            payload = json.loads(
                b''.join(chunks).decode('utf-8'),
                object_pairs_hook=_reject_duplicate_json_keys,
                parse_constant=_reject_nonfinite_json_value,
            )
            if not isinstance(payload, dict):
                raise KISTokenCacheError('KIS token cache JSON must be an object')
        except KISTokenCacheError:
            raise
        except (json.JSONDecodeError, OSError, RecursionError, UnicodeError, ValueError) as exc:
            raise KISTokenCacheError('KIS token cache JSON is invalid') from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
            os.close(directory_descriptor)
        return payload

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        directory_descriptor = self._open_cache_directory(create=True)
        if directory_descriptor is None:
            raise KISTokenCacheError('KIS token cache directory could not be created')
        temporary_name = f'.{path.name}.{uuid.uuid4().hex}.tmp'
        descriptor: int | None = None
        try:
            serialized = json.dumps(
                payload,
                sort_keys=True,
                separators=(',', ':'),
                allow_nan=False,
            ).encode('utf-8')
        except (TypeError, ValueError) as exc:
            os.close(directory_descriptor)
            raise KISTokenCacheError('KIS token cache record is not valid JSON') from exc
        if len(serialized) > _CACHE_FILE_MAX_BYTES:
            os.close(directory_descriptor)
            raise KISTokenCacheError('KIS token cache record is unexpectedly large')
        try:
            descriptor = os.open(
                temporary_name,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, 'O_NOFOLLOW', 0),
                0o600,
                dir_fd=directory_descriptor,
            )
            os.fchmod(descriptor, 0o600)
            written = 0
            while written < len(serialized):
                written += os.write(descriptor, serialized[written:])
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            os.replace(
                temporary_name,
                path.name,
                src_dir_fd=directory_descriptor,
                dst_dir_fd=directory_descriptor,
            )
            os.fsync(directory_descriptor)
        except OSError as exc:
            raise KISTokenCacheError('KIS token cache record could not be written safely') from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                os.unlink(temporary_name, dir_fd=directory_descriptor)
            except FileNotFoundError:
                pass
            os.close(directory_descriptor)

    @staticmethod
    def _is_safe_private_file(file_stat: os.stat_result) -> bool:
        return bool(
            stat.S_ISREG(file_stat.st_mode)
            and (not hasattr(os, 'getuid') or file_stat.st_uid == os.getuid())
            and stat.S_IMODE(file_stat.st_mode) == 0o600
            and file_stat.st_nlink == 1
        )


def _require_timezone_aware(*values: datetime) -> None:
    if any(
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        for value in values
    ):
        raise KISTokenCacheError(
            'KIS token cache write timestamps must be timezone-aware'
        )


def _parse_utc_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def is_valid_bearer_token(value: object) -> bool:
    if not isinstance(value, str) or not value or len(value) > 8192 or not value.isascii():
        return False
    token_body = value.rstrip('=')
    if not token_body or value[len(token_body) :] != '=' * (len(value) - len(token_body)):
        return False
    token68_characters = frozenset(
        'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~+/'
    )
    return all(character in token68_characters for character in token_body)


def _is_uuid_text(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(uuid.UUID(value)) == value
    except (ValueError, AttributeError):
        return False


def _is_sha256_fingerprint(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith('sha256:'):
        return False
    digest = value.removeprefix('sha256:')
    return len(digest) == 64 and all(character in '0123456789abcdef' for character in digest)


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key')
        result[key] = value
    return result


def _reject_nonfinite_json_value(value: str) -> None:
    raise ValueError(f'non-finite JSON value: {value}')
