from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from app.broker.base import BrokerAdapter, BrokerAdapterError, BrokerConfig
from app.broker.kis import KisBrokerAdapter
from app.core.config import Settings
from app.models.runtime import BrokerSession, BrokerSessionEvent

DEFAULT_BROKER_SOURCE = "operator"
ACCESS_TOKEN_REUSE_MARGIN = timedelta(minutes=5)


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


def build_broker_adapter(settings: Settings) -> BrokerAdapter:
    return KisBrokerAdapter(settings=settings)


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
    return _session_snapshot(config=config, state=state, reconnect=reconnect)


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

    if _session_token_reusable(state):
        return state

    previous_state = _session_snapshot(config=config, state=state, reconnect=reconnect)
    try:
        grant = broker_adapter.authenticate()
    except BrokerAdapterError as exc:
        state.last_error = str(exc)
        state.last_error_at = datetime.now(UTC)
        _record_event(
            db=db,
            state=state,
            action="authenticate",
            outcome="rejected",
            actor=None,
            source=source,
            reason=reason,
            message=str(exc),
            previous_state=previous_state,
            current_state=_session_snapshot(config=config, state=state, reconnect=reconnect),
        )
        raise

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
    state = ensure_authenticated_broker_session(
        settings=settings,
        db=db,
        adapter=broker_adapter,
        source=source,
        reason=reason,
    )
    reconnect = broker_adapter.reconnect_handle()

    if state.approval_key:
        return state

    previous_state = _session_snapshot(config=config, state=state, reconnect=reconnect)
    try:
        approval = broker_adapter.issue_realtime_approval(state.access_token or "")
    except BrokerAdapterError as exc:
        state.last_error = str(exc)
        state.last_error_at = datetime.now(UTC)
        _record_event(
            db=db,
            state=state,
            action="issue_realtime_approval",
            outcome="rejected",
            actor=None,
            source=source,
            reason=reason,
            message=str(exc),
            previous_state=previous_state,
            current_state=_session_snapshot(config=config, state=state, reconnect=reconnect),
        )
        raise

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
        current_state=_session_snapshot(config=config, state=state, reconnect=reconnect),
    )
    db.refresh(state)
    return state


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

    if not command.force and _session_token_reusable(state):
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
        grant = broker_adapter.authenticate()
    except BrokerAdapterError as exc:
        state.last_error = str(exc)
        state.last_error_at = datetime.now(UTC)
        return _finalize_action(
            db=db,
            state=state,
            config=config,
            reconnect=reconnect,
            command=command,
            action="authenticate",
            outcome="rejected",
            message=str(exc),
            status_code=502,
            previous_state=previous_state,
        )

    _apply_token_grant(state=state, config=config, grant=grant)

    return _finalize_action(
        db=db,
        state=state,
        config=config,
        reconnect=reconnect,
        command=command,
        action="authenticate",
        outcome="applied",
        message="broker access token issued",
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

    if not state.access_token:
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

    try:
        revocation = broker_adapter.revoke(state.access_token)
    except BrokerAdapterError as exc:
        state.last_error = str(exc)
        state.last_error_at = datetime.now(UTC)
        return _finalize_action(
            db=db,
            state=state,
            config=config,
            reconnect=reconnect,
            command=command,
            action="revoke",
            outcome="rejected",
            message=str(exc),
            status_code=502,
            previous_state=previous_state,
        )

    state.access_token = None
    state.token_type = None
    state.access_token_expires_at = None
    state.refresh_token = None
    state.refresh_token_expires_at = None
    state.approval_key = None
    state.approval_key_issued_at = None
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


def _ensure_session(*, db: Session, config: BrokerConfig) -> BrokerSession:
    session_id = _session_id(config)
    state = db.get(BrokerSession, session_id)
    if state is None:
        state = BrokerSession(
            id=session_id,
            provider=config.provider,
            mode=config.mode,
            api_base_url=config.api_base_url,
        )
        db.add(state)
        db.flush()
        return state

    if state.api_base_url != config.api_base_url:
        state.api_base_url = config.api_base_url
    return state


def _load_session(*, db: Session, config: BrokerConfig) -> BrokerSession | None:
    return db.get(BrokerSession, _session_id(config))


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
        },
    )


def _apply_token_grant(
    *,
    state: BrokerSession,
    config: BrokerConfig,
    grant,
) -> None:
    state.api_base_url = config.api_base_url
    state.access_token = grant.access_token
    state.token_type = grant.token_type
    state.access_token_expires_at = _as_utc(grant.access_token_expires_at)
    state.refresh_token = grant.refresh_token
    state.refresh_token_expires_at = _as_utc(grant.refresh_token_expires_at)
    state.last_authenticated_at = datetime.now(UTC)
    state.last_error = None
    state.last_error_at = None


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
