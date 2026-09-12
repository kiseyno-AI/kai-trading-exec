from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.broker.base import (
    BrokerAdapter,
    BrokerAdapterError,
    BrokerConfig,
    BrokerErrorClassification,
    BrokerTokenRevocation,
)
from app.broker.error_safety import safe_broker_error_code, safe_broker_error_message
from app.broker.alpaca import AlpacaBrokerAdapter
from app.broker.kis import KisBrokerAdapter
from app.broker.kis_token_cache import KISTokenRevocationClaimResult
from app.broker.kis_token_coordinator import (
    begin_shared_kis_token_revocation,
    finalize_shared_kis_token_revocation,
    issue_shared_kis_token,
    load_shared_kis_token_for_revocation,
    read_shared_kis_token_status,
    renew_shared_kis_token_revocation,
    shared_kis_token_revocation_is_finalized,
)
from app.broker.upbit import UpbitBrokerAdapter
from app.core.config import Settings
from app.models.runtime import BrokerSession, BrokerSessionEvent

DEFAULT_BROKER_SOURCE = "operator"
ACCESS_TOKEN_REUSE_MARGIN = timedelta(minutes=5)
REALTIME_APPROVAL_GENERATION_RETRY_LIMIT = 3


@dataclass(frozen=True)
class BrokerSessionCommand:
    actor: str | None = None
    reason: str | None = None
    source: str = DEFAULT_BROKER_SOURCE
    force: bool = False


@dataclass(frozen=True)
class BrokerSessionActionResult:
    status_code: int
    payload: dict[str, object]


def build_broker_adapter(settings: Settings, *, provider: str = "kis") -> BrokerAdapter:
    normalized_provider = provider.strip().lower()
    if normalized_provider == "kis":
        return KisBrokerAdapter(settings=settings)
    if normalized_provider == "upbit":
        return UpbitBrokerAdapter(settings=settings)
    if normalized_provider == "alpaca":
        return AlpacaBrokerAdapter(settings=settings)
    raise BrokerAdapterError(
        f"unsupported broker provider: {normalized_provider or '<empty>'}",
        status_code=422,
    )


def build_broker_session_summary(
    *,
    settings: Settings,
    db: Session | None = None,
    adapter: BrokerAdapter | None = None,
) -> dict[str, object]:
    broker_adapter = adapter or build_broker_adapter(settings)
    config = broker_adapter.describe()
    state = _load_session(db=db, config=config) if db is not None else None
    reconnect = broker_adapter.reconnect_handle()
    snapshot = _session_snapshot(config=config, state=state, reconnect=reconnect)
    if isinstance(broker_adapter, KisBrokerAdapter) and config.configured:
        try:
            shared_status = read_shared_kis_token_status(settings)
        except BrokerAdapterError as exc:
            snapshot.update(
                {
                    "status": "error",
                    "reason": safe_broker_error_message(exc),
                    "access_token_present": False,
                    "access_token_expires_at": None,
                    "token_generation": None,
                    "token_refresh_after": None,
                    "refresh_token_present": False,
                    "refresh_token_expires_at": None,
                    "approval_key_present": False,
                    "approval_key_issued_at": None,
                }
            )
        else:
            shared_session_is_current = bool(
                shared_status.status == "active"
                and state is not None
                and state.generation is not None
                and state.generation == shared_status.generation
            )
            snapshot.update(
                {
                    "status": shared_status.status,
                    "reason": shared_status.reason,
                    "access_token_present": shared_status.access_token_present,
                    "access_token_expires_at": _serialize_datetime(
                        shared_status.expires_at
                    ),
                    "token_generation": shared_status.generation,
                    "token_refresh_after": _serialize_datetime(
                        shared_status.refresh_after
                    ),
                    "refresh_token_present": False,
                    "refresh_token_expires_at": None,
                    "approval_key_present": bool(
                        shared_session_is_current and state.approval_key
                    ),
                    "approval_key_issued_at": (
                        _serialize_datetime(state.approval_key_issued_at)
                        if shared_session_is_current
                        else None
                    ),
                }
            )
    return snapshot


def ensure_authenticated_broker_session(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter | None = None,
    source: str = "system",
    reason: str = "broker order submission requires an active session",
) -> BrokerSession:
    broker_adapter = adapter or build_broker_adapter(settings)
    config = broker_adapter.describe()
    state = _ensure_session(db=db, config=config)
    reconnect = broker_adapter.reconnect_handle()

    if not config.configured:
        raise BrokerAdapterError(config.reason or "broker credentials are unavailable", status_code=409)

    if not isinstance(broker_adapter, KisBrokerAdapter) and _session_token_reusable(state):
        return state

    previous_state = _session_snapshot(config=config, state=state, reconnect=reconnect)
    try:
        grant = _authenticate_grant(
            settings=settings,
            broker_adapter=broker_adapter,
            force=False,
        )
    except BrokerAdapterError as exc:
        safe_message = safe_broker_error_message(exc)
        state.last_error = safe_message
        state.last_error_at = datetime.now(UTC)
        _record_event(
            db=db,
            state=state,
            action="authenticate",
            outcome="rejected",
            actor=None,
            source=source,
            reason=reason,
            message=safe_message,
            previous_state=previous_state,
            current_state=_session_snapshot(config=config, state=state, reconnect=reconnect),
        )
        raise

    state = _lock_session_for_update(db=db, state=state)
    if isinstance(broker_adapter, KisBrokerAdapter):
        grant = _authenticate_grant(
            settings=settings,
            broker_adapter=broker_adapter,
            force=False,
        )
    previous_state = _session_snapshot(config=config, state=state, reconnect=reconnect)
    if _grant_is_already_mirrored(state=state, grant=grant):
        db.commit()
        db.refresh(state)
        return state
    _apply_token_grant(state=state, config=config, grant=grant)
    _record_event(
        db=db,
        state=state,
        action="authenticate",
        outcome="applied",
        actor=None,
        source=source,
        reason=reason,
        message="broker access token issued",
        previous_state=previous_state,
        current_state=_session_snapshot(config=config, state=state, reconnect=reconnect),
    )
    db.refresh(state)
    return state


def ensure_realtime_approval(
    *,
    settings: Settings,
    db: Session,
    adapter: BrokerAdapter | None = None,
    source: str = "system",
    reason: str = "realtime connectivity requires an approval key",
) -> BrokerSession:
    broker_adapter = adapter or build_broker_adapter(settings)
    config = broker_adapter.describe()
    reconnect = broker_adapter.reconnect_handle()
    for _attempt in range(REALTIME_APPROVAL_GENERATION_RETRY_LIMIT):
        state = ensure_authenticated_broker_session(
            settings=settings,
            db=db,
            adapter=broker_adapter,
            source=source,
            reason=reason,
        )
        if state.approval_key:
            return state

        expected_access_token = state.access_token or ""
        expected_generation = state.generation
        previous_state = _session_snapshot(
            config=config,
            state=state,
            reconnect=reconnect,
        )
        try:
            approval = broker_adapter.issue_realtime_approval(expected_access_token)
        except BrokerAdapterError as exc:
            state = _lock_session_for_update(db=db, state=state)
            if not _session_matches_token_generation(
                state=state,
                access_token=expected_access_token,
                generation=expected_generation,
            ):
                db.commit()
                continue
            safe_message = safe_broker_error_message(exc)
            state.last_error = safe_message
            state.last_error_at = datetime.now(UTC)
            _record_event(
                db=db,
                state=state,
                action="issue_realtime_approval",
                outcome="rejected",
                actor=None,
                source=source,
                reason=reason,
                message=safe_message,
                previous_state=previous_state,
                current_state=_session_snapshot(
                    config=config,
                    state=state,
                    reconnect=reconnect,
                ),
            )
            raise

        state = _lock_session_for_update(db=db, state=state)
        if isinstance(broker_adapter, KisBrokerAdapter):
            authoritative_grant = _authenticate_grant(
                settings=settings,
                broker_adapter=broker_adapter,
                force=False,
            )
            if not _grant_is_already_mirrored(
                state=state,
                grant=authoritative_grant,
            ):
                _apply_token_grant(
                    state=state,
                    config=config,
                    grant=authoritative_grant,
                )
        if not _session_matches_token_generation(
            state=state,
            access_token=expected_access_token,
            generation=expected_generation,
        ):
            db.commit()
            db.refresh(state)
            continue

        state.approval_key = approval.approval_key
        state.approval_key_issued_at = _as_utc(approval.issued_at)
        state.last_error = None
        state.last_error_at = None
        _record_event(
            db=db,
            state=state,
            action="issue_realtime_approval",
            outcome="applied",
            actor=None,
            source=source,
            reason=reason,
            message="broker realtime approval key issued",
            previous_state=previous_state,
            current_state=_session_snapshot(
                config=config,
                state=state,
                reconnect=reconnect,
            ),
        )
        db.refresh(state)
        return state

    raise BrokerAdapterError(
        "broker token generation changed while issuing realtime approval",
        status_code=409,
        retryable=True,
    )


def authenticate_broker_session(
    *,
    settings: Settings,
    db: Session,
    command: BrokerSessionCommand,
    adapter: BrokerAdapter | None = None,
) -> BrokerSessionActionResult:
    broker_adapter = adapter or build_broker_adapter(settings)
    config = broker_adapter.describe()
    reconnect = broker_adapter.reconnect_handle()
    state = _ensure_session(db=db, config=config)
    previous_state = _session_snapshot(config=config, state=state, reconnect=reconnect)

    if not config.configured:
        return _finalize_action(
            db=db,
            state=state,
            config=config,
            reconnect=reconnect,
            command=command,
            action="authenticate",
            outcome="rejected",
            message=config.reason or "broker credentials are unavailable",
            status_code=409,
            previous_state=previous_state,
        )

    if (
        not isinstance(broker_adapter, KisBrokerAdapter)
        and not command.force
        and _session_token_reusable(state)
    ):
        return _finalize_action(
            db=db,
            state=state,
            config=config,
            reconnect=reconnect,
            command=command,
            action="authenticate",
            outcome="noop",
            message="broker access token is already active",
            status_code=200,
            previous_state=previous_state,
        )

    try:
        grant = _authenticate_grant(
            settings=settings,
            broker_adapter=broker_adapter,
            force=command.force,
        )
    except BrokerAdapterError as exc:
        return _finalize_broker_error_action(
            db=db,
            state=state,
            config=config,
            reconnect=reconnect,
            command=command,
            action="authenticate",
            previous_state=previous_state,
            error=exc,
        )

    state = _lock_session_for_update(db=db, state=state)
    if isinstance(broker_adapter, KisBrokerAdapter):
        grant = _authenticate_grant(
            settings=settings,
            broker_adapter=broker_adapter,
            force=False,
        )
    previous_state = _session_snapshot(config=config, state=state, reconnect=reconnect)
    unchanged = _grant_is_already_mirrored(state=state, grant=grant)
    _apply_token_grant(state=state, config=config, grant=grant)

    return _finalize_action(
        db=db,
        state=state,
        config=config,
        reconnect=reconnect,
        command=command,
        action="authenticate",
        outcome="noop" if unchanged else "applied",
        message=(
            "shared broker access token is already mirrored"
            if unchanged
            else "broker access token issued"
        ),
        status_code=200,
        previous_state=previous_state,
    )


def revoke_broker_session(
    *,
    settings: Settings,
    db: Session,
    command: BrokerSessionCommand,
    adapter: BrokerAdapter | None = None,
) -> BrokerSessionActionResult:
    broker_adapter = adapter or build_broker_adapter(settings)
    config = broker_adapter.describe()
    reconnect = broker_adapter.reconnect_handle()
    state = _ensure_session(db=db, config=config)
    previous_state = _session_snapshot(config=config, state=state, reconnect=reconnect)

    if (
        isinstance(broker_adapter, KisBrokerAdapter)
        and not config.configured
        and state.access_token
    ):
        return _finalize_action(
            db=db,
            state=state,
            config=config,
            reconnect=reconnect,
            command=command,
            action="revoke",
            outcome="rejected",
            message=config.reason or "broker credentials are unavailable",
            status_code=409,
            previous_state=previous_state,
        )

    shared_grant = None
    if isinstance(broker_adapter, KisBrokerAdapter) and config.configured:
        try:
            shared_grant = load_shared_kis_token_for_revocation(settings)
        except BrokerAdapterError as exc:
            safe_message = safe_broker_error_message(exc)
            state.last_error = safe_message
            state.last_error_at = datetime.now(UTC)
            return _finalize_action(
                db=db,
                state=state,
                config=config,
                reconnect=reconnect,
                command=command,
                action="revoke",
                outcome="rejected",
                message=safe_message,
                status_code=502,
                previous_state=previous_state,
            )

    if isinstance(broker_adapter, KisBrokerAdapter) and shared_grant is None:
        state = _lock_session_for_update(db=db, state=state)
        current_shared_grant = load_shared_kis_token_for_revocation(settings)
        if current_shared_grant is not None:
            _apply_token_grant(
                state=state,
                config=config,
                grant=current_shared_grant,
            )
            db.commit()
            db.refresh(state)
            return _finalize_action(
                db=db,
                state=state,
                config=config,
                reconnect=reconnect,
                command=command,
                action="revoke",
                outcome="noop",
                message=(
                    "a newer shared broker token appeared during revoke; "
                    "the newer generation was preserved"
                ),
                status_code=200,
                previous_state=previous_state,
            )
        if state.access_token:
            _clear_token_state(state)
        return _finalize_action(
            db=db,
            state=state,
            config=config,
            reconnect=reconnect,
            command=command,
            action="revoke",
            outcome="noop",
            message="no authoritative shared broker access token is stored",
            status_code=200,
            previous_state=previous_state,
        )

    if not isinstance(broker_adapter, KisBrokerAdapter) and not state.access_token:
        return _finalize_action(
            db=db,
            state=state,
            config=config,
            reconnect=reconnect,
            command=command,
            action="revoke",
            outcome="noop",
            message="no broker access token is currently stored",
            status_code=200,
            previous_state=previous_state,
        )

    if not config.configured:
        return _finalize_action(
            db=db,
            state=state,
            config=config,
            reconnect=reconnect,
            command=command,
            action="revoke",
            outcome="rejected",
            message=config.reason or "broker credentials are unavailable",
            status_code=409,
            previous_state=previous_state,
        )

    token_to_revoke = (
        shared_grant.access_token
        if shared_grant is not None
        else state.access_token
    )
    assert token_to_revoke is not None
    generation_to_revoke = (
        shared_grant.generation
        if shared_grant is not None
        else state.generation
    )
    locally_fenced = False
    try:
        if isinstance(broker_adapter, KisBrokerAdapter):
            if generation_to_revoke is None:
                raise BrokerAdapterError(
                    "stored KIS token generation is missing",
                    status_code=409,
                )
            revocation_claim = begin_shared_kis_token_revocation(
                settings=settings,
                access_token=token_to_revoke,
                generation=generation_to_revoke,
            )
            if revocation_claim.outcome == KISTokenRevocationClaimResult.IN_PROGRESS:
                raise BrokerAdapterError(
                    "KIS shared token revocation is already in progress",
                    status_code=503,
                    classification=BrokerErrorClassification.TRANSIENT,
                    retryable=True,
                )
            if (
                revocation_claim.outcome
                != KISTokenRevocationClaimResult.ACQUIRED
                or revocation_claim.claim_id is None
            ):
                raise BrokerAdapterError(
                    "stored KIS token is not the authoritative shared token",
                    status_code=409,
                )
            state = _lock_session_for_update(db=db, state=state)
            current_shared_grant = load_shared_kis_token_for_revocation(settings)
            if (
                current_shared_grant is not None
                and current_shared_grant.generation != generation_to_revoke
            ):
                _apply_token_grant(
                    state=state,
                    config=config,
                    grant=current_shared_grant,
                )
            elif _session_matches_token_generation(
                state=state,
                access_token=token_to_revoke,
                generation=generation_to_revoke,
            ):
                _fence_token_state(state)
            db.commit()
            db.refresh(state)
            locally_fenced = True
            renewed = renew_shared_kis_token_revocation(
                settings=settings,
                access_token=token_to_revoke,
                generation=generation_to_revoke,
                claim_id=revocation_claim.claim_id,
            )
            if not renewed:
                raise BrokerAdapterError(
                    "KIS shared token revocation claim was lost before remote revoke",
                    status_code=503,
                    classification=BrokerErrorClassification.TRANSIENT,
                    retryable=True,
                )
        revocation = broker_adapter.revoke(token_to_revoke)
        if isinstance(broker_adapter, KisBrokerAdapter):
            finalized = finalize_shared_kis_token_revocation(
                settings=settings,
                access_token=token_to_revoke,
                generation=generation_to_revoke,
                claim_id=revocation_claim.claim_id,
            )
            if not finalized:
                current_shared_grant = load_shared_kis_token_for_revocation(settings)
                if (
                    current_shared_grant is not None
                    and current_shared_grant.generation == generation_to_revoke
                ):
                    raise BrokerAdapterError(
                        "KIS shared token revocation could not be finalized",
                        status_code=409,
                    )
    except BrokerAdapterError as exc:
        completed_concurrently = False
        if (
            isinstance(broker_adapter, KisBrokerAdapter)
            and generation_to_revoke is not None
        ):
            try:
                completed_concurrently = shared_kis_token_revocation_is_finalized(
                    settings=settings,
                    access_token=token_to_revoke,
                    generation=generation_to_revoke,
                )
            except BrokerAdapterError:
                pass
        if completed_concurrently:
            revocation = BrokerTokenRevocation(
                message="broker token revocation completed by another request"
            )
        else:
            return _finalize_broker_error_action(
                db=db,
                state=state,
                config=config,
                reconnect=reconnect,
                command=command,
                action="revoke",
                previous_state=previous_state,
                error=exc,
            )

    if not locally_fenced:
        _clear_token_state(state)
    else:
        state = _lock_session_for_update(db=db, state=state)
        current_shared_grant = load_shared_kis_token_for_revocation(settings)
        if (
            current_shared_grant is not None
            and current_shared_grant.generation != generation_to_revoke
        ):
            _apply_token_grant(
                state=state,
                config=config,
                grant=current_shared_grant,
            )
        elif _session_matches_token_generation(
            state=state,
            access_token=token_to_revoke,
            generation=generation_to_revoke,
        ):
            _fence_token_state(state)
        if state.access_token is None:
            state.last_revoked_at = datetime.now(UTC)
    state.last_error = None
    state.last_error_at = None

    return _finalize_action(
        db=db,
        state=state,
        config=config,
        reconnect=reconnect,
        command=command,
        action="revoke",
        outcome="applied",
        message=revocation.message,
        status_code=200,
        previous_state=previous_state,
    )


def _session_token_reusable(state: BrokerSession) -> bool:
    if state.access_token is None:
        return False
    if state.access_token_expires_at is None:
        return True
    return _as_utc(state.access_token_expires_at) > datetime.now(UTC) + ACCESS_TOKEN_REUSE_MARGIN


def _session_matches_token_generation(
    *,
    state: BrokerSession,
    access_token: str,
    generation: str | None,
) -> bool:
    return state.access_token == access_token and state.generation == generation


def _clear_token_state(state: BrokerSession) -> None:
    _fence_token_state(state)
    state.last_revoked_at = datetime.now(UTC)


def _fence_token_state(state: BrokerSession) -> None:
    state.access_token = None
    state.token_type = None
    state.access_token_expires_at = None
    state.generation = None
    state.refresh_after = None
    state.refresh_token = None
    state.refresh_token_expires_at = None
    state.approval_key = None
    state.approval_key_issued_at = None


def _ensure_session(*, db: Session, config: BrokerConfig) -> BrokerSession:
    session_id = _session_id(config)
    state = db.get(BrokerSession, session_id)
    if state is None:
        values = {
            "id": session_id,
            "provider": config.provider,
            "mode": config.mode,
            "api_base_url": config.api_base_url,
        }
        dialect_name = db.get_bind().dialect.name
        if dialect_name == "sqlite":
            db.execute(
                sqlite_insert(BrokerSession)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[BrokerSession.id])
            )
        elif dialect_name == "postgresql":
            db.execute(
                postgresql_insert(BrokerSession)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[BrokerSession.id])
            )
        else:
            candidate = BrokerSession(**values)
            try:
                with db.begin_nested():
                    db.add(candidate)
                    db.flush()
            except IntegrityError:
                pass
        state = db.get(BrokerSession, session_id)
        if state is None:
            candidate = BrokerSession(**values)
            try:
                with db.begin_nested():
                    db.add(candidate)
                    db.flush()
            except IntegrityError:
                state = db.get(BrokerSession, session_id)
                if state is None:
                    raise
            else:
                state = candidate

    if state.api_base_url != config.api_base_url:
        state.api_base_url = config.api_base_url
    return state


def _load_session(*, db: Session, config: BrokerConfig) -> BrokerSession | None:
    return db.get(BrokerSession, _session_id(config))


def _lock_session_for_update(*, db: Session, state: BrokerSession) -> BrokerSession:
    if db.get_bind().dialect.name == "sqlite":
        db.execute(
            update(BrokerSession)
            .where(BrokerSession.id == state.id)
            .values(api_base_url=BrokerSession.api_base_url)
        )
    locked_state = db.scalar(
        select(BrokerSession)
        .where(BrokerSession.id == state.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if locked_state is None:
        raise RuntimeError("broker session disappeared during authentication")
    db.refresh(locked_state)
    return locked_state


def _session_id(config: BrokerConfig) -> str:
    return f"{config.provider}:{config.mode}"


def _session_snapshot(
    *,
    config: BrokerConfig,
    state: BrokerSession | None,
    reconnect,
) -> dict[str, object]:
    now = datetime.now(UTC)

    if not config.configured:
        status = "disabled" if config.mode == "mock" else "missing_credentials"
        reason = config.reason
    elif state is None:
        status = "uninitialized"
        reason = "no broker access token has been issued"
    elif state.access_token and _token_is_active(state, now=now):
        status = "active"
        reason = None
    elif state.access_token and state.access_token_expires_at is not None:
        status = "expired"
        reason = "stored broker access token has expired"
    elif state.last_error:
        status = "error"
        reason = state.last_error
    elif state.last_revoked_at is not None:
        status = "revoked"
        reason = "broker access token was revoked"
    else:
        status = "uninitialized"
        reason = "no broker access token has been issued"

    return {
        "provider": config.provider,
        "mode": config.mode,
        "configured": config.configured,
        "status": status,
        "reason": reason,
        "api_base_url": config.api_base_url,
        "access_token_present": bool(state and state.access_token),
        "access_token_expires_at": _serialize_datetime(
            state.access_token_expires_at if state else None
        ),
        "token_generation": state.generation if state else None,
        "token_refresh_after": _serialize_datetime(
            state.refresh_after if state else None
        ),
        "refresh_token_present": bool(state and state.refresh_token),
        "refresh_token_expires_at": _serialize_datetime(
            state.refresh_token_expires_at if state else None
        ),
        "approval_key_present": bool(state and state.approval_key),
        "approval_key_issued_at": _serialize_datetime(
            state.approval_key_issued_at if state else None
        ),
        "token_type": state.token_type if state else None,
        "last_authenticated_at": _serialize_datetime(
            state.last_authenticated_at if state else None
        ),
        "last_revoked_at": _serialize_datetime(state.last_revoked_at if state else None),
        "last_error": state.last_error if state else None,
        "last_error_at": _serialize_datetime(state.last_error_at if state else None),
        "shared_realtime_subscriptions_supported": (
            config.shared_realtime_subscriptions_supported
        ),
        "reconnect_supported": config.reconnect_supported,
        "bounded_rest_reconciliation_supported": (
            config.bounded_rest_reconciliation_supported
        ),
        "websocket_approval_path": config.websocket_approval_path,
        "reconnect_strategy": reconnect.strategy,
        "updated_at": _serialize_datetime(state.updated_at if state else None),
    }


def _token_is_active(
    state: BrokerSession,
    *,
    now: datetime,
) -> bool:
    if state.access_token is None:
        return False
    if state.access_token_expires_at is None:
        return True
    return _as_utc(state.access_token_expires_at) > now


def _finalize_action(
    *,
    db: Session,
    state: BrokerSession,
    config: BrokerConfig,
    reconnect,
    command: BrokerSessionCommand,
    action: str,
    outcome: str,
    message: str,
    status_code: int,
    previous_state: dict[str, object],
    outcome_unknown: bool = False,
    retry_required: bool = False,
    error_code: str | None = None,
    session_authoritative: bool = True,
) -> BrokerSessionActionResult:
    current_state = _session_snapshot(config=config, state=state, reconnect=reconnect)
    event = _record_event(
        db=db,
        state=state,
        action=action,
        outcome=outcome,
        actor=command.actor,
        source=command.source,
        reason=command.reason,
        message=message,
        previous_state=previous_state,
        current_state=current_state,
    )
    db.refresh(state)

    return BrokerSessionActionResult(
        status_code=status_code,
        payload={
            "action": action,
            "outcome": outcome,
            "changed": outcome == "applied",
            "message": message,
            "event_id": event.id,
            "session": _session_snapshot(config=config, state=state, reconnect=reconnect),
            "outcome_unknown": outcome_unknown,
            "retry_required": retry_required,
            "error_code": error_code,
            "session_authoritative": session_authoritative,
        },
    )


def _finalize_broker_error_action(
    *,
    db: Session,
    state: BrokerSession,
    config: BrokerConfig,
    reconnect,
    command: BrokerSessionCommand,
    action: str,
    previous_state: dict[str, object],
    error: BrokerAdapterError,
) -> BrokerSessionActionResult:
    safe_message = safe_broker_error_message(error)
    state.last_error = safe_message
    state.last_error_at = datetime.now(UTC)
    response_metadata = (
        error.response_metadata
        if isinstance(error.response_metadata, dict)
        else {}
    )
    outcome_unknown = bool(
        error.code == "KIS_TOKEN_CACHE_OUTCOME_UNKNOWN"
        or response_metadata.get("outcome_unknown") is True
    )
    retry_required = bool(
        outcome_unknown
        or error.retryable
        or response_metadata.get("retry_required") is True
    )
    return _finalize_action(
        db=db,
        state=state,
        config=config,
        reconnect=reconnect,
        command=command,
        action=action,
        outcome="rejected",
        message=safe_message,
        status_code=_broker_action_error_status_code(
            error,
            outcome_unknown=outcome_unknown,
        ),
        previous_state=previous_state,
        outcome_unknown=outcome_unknown,
        retry_required=retry_required,
        error_code=safe_broker_error_code(error.code, provider=config.provider),
        session_authoritative=not outcome_unknown,
    )


def _broker_action_error_status_code(
    error: BrokerAdapterError,
    *,
    outcome_unknown: bool,
) -> int:
    # Preserve the pre-existing public contract for ordinary broker failures.
    # Only an indeterminate token-cache mutation carries its original retryable
    # status so callers can distinguish it from a definitive upstream failure.
    if not outcome_unknown:
        return 502
    status_code = error.status_code
    if (
        isinstance(status_code, int)
        and not isinstance(status_code, bool)
        and 400 <= status_code <= 599
    ):
        return status_code
    return 503


def _apply_token_grant(
    *,
    state: BrokerSession,
    config: BrokerConfig,
    grant,
) -> None:
    token_changed = state.access_token != grant.access_token
    generation_changed = state.generation != grant.generation
    if token_changed or generation_changed:
        state.approval_key = None
        state.approval_key_issued_at = None
    state.api_base_url = config.api_base_url
    state.access_token = grant.access_token
    state.token_type = grant.token_type
    state.access_token_expires_at = _as_utc(grant.access_token_expires_at)
    state.refresh_token = grant.refresh_token
    state.refresh_token_expires_at = _as_utc(grant.refresh_token_expires_at)
    state.generation = grant.generation
    state.refresh_after = _as_utc(grant.refresh_after)
    state.last_authenticated_at = datetime.now(UTC)
    state.last_error = None
    state.last_error_at = None


def _authenticate_grant(
    *,
    settings: Settings,
    broker_adapter: BrokerAdapter,
    force: bool,
):
    if isinstance(broker_adapter, KisBrokerAdapter):
        return issue_shared_kis_token(
            settings=settings,
            issue_token=broker_adapter.authenticate,
            force=force,
        )
    return broker_adapter.authenticate()


def _grant_is_already_mirrored(*, state: BrokerSession, grant) -> bool:
    return bool(
        state.access_token == grant.access_token
        and state.access_token_expires_at is not None
        and _as_utc(state.access_token_expires_at)
        == _as_utc(grant.access_token_expires_at)
        and state.generation == grant.generation
        and _as_utc(state.refresh_after) == _as_utc(grant.refresh_after)
    )


def _record_event(
    *,
    db: Session,
    state: BrokerSession,
    action: str,
    outcome: str,
    actor: str | None,
    source: str,
    reason: str | None,
    message: str,
    previous_state: dict[str, object],
    current_state: dict[str, object],
) -> BrokerSessionEvent:
    event = BrokerSessionEvent(
        broker_session_id=state.id,
        action=action,
        outcome=outcome,
        actor=actor,
        source=source,
        reason=reason,
        message=message,
        previous_state=previous_state,
        current_state=current_state,
        occurred_at=datetime.now(UTC),
    )
    db.add(event)
    db.commit()
    return event


def _serialize_datetime(value: datetime | None) -> str | None:
    normalized = _as_utc(value)
    return normalized.isoformat() if normalized is not None else None


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
