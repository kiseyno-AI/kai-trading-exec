import threading
from types import SimpleNamespace

import pytest

from app.core.config import Settings
from app.models.runtime import ExecutionPlanItem
from app.services import core_revocation_exposure
from app.services.account_drawdown_recovery import recovery_account_fingerprint
from app.services.order_intent_serialization import (
    order_intent_file_lock,
    order_intent_process_lock,
    order_intent_scope_key_for_account,
)
from app.services.position_identity import PositionIdentity


class _LockSession:
    def __init__(self, *, dialect: str, acquired: object = True) -> None:
        self._bind = SimpleNamespace(dialect=SimpleNamespace(name=dialect))
        self._acquired = acquired
        self.statement: object | None = None
        self.parameters: dict[str, object] | None = None
        self.driver_sql: list[str] = []
        self.executed: list[object] = []

    def get_bind(self) -> object:
        return self._bind

    def connection(self) -> "_LockSession":
        return self

    def exec_driver_sql(self, statement: str) -> None:
        self.driver_sql.append(statement)

    def execute(self, statement: object) -> None:
        self.executed.append(statement)

    def scalar(
        self,
        statement: object,
        parameters: dict[str, object],
    ) -> object:
        self.statement = statement
        self.parameters = parameters
        return self._acquired


def test_revocation_serialization_reserves_exact_kis_krw_scope_and_positions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        database_url="postgresql+psycopg://user:pass@localhost/runtime",
        runtime_mode="live",
    )
    identity = PositionIdentity(
        provider="kis",
        account_scope="opaque-account-scope",
        exchange="KRX",
    )
    monkeypatch.setattr(
        core_revocation_exposure,
        "_expected_identity",
        lambda **_kwargs: identity,
    )
    db = _LockSession(dialect="postgresql")

    with core_revocation_exposure.serialized_revocation_exposure(
        db=db,  # type: ignore[arg-type]
        settings=settings,
        items=[
            ExecutionPlanItem(
                id="entry",
                execution_plan_id="plan",
                plan_item_index=0,
                symbol="029460",
                side="buy",
                intent="next_open_buy",
                provider="kis",
                venue="KRX",
            )
        ],
    ):
        pass

    assert db.statement is not None
    assert "pg_try_advisory_xact_lock" in str(db.statement)
    assert db.parameters == {
        "scope": order_intent_scope_key_for_account(
            provider="kis",
            runtime_mode="live",
            account_scope=identity.account_scope,
            market="KRW",
        )
    }
    assert len(db.executed) == 1
    assert "LOCK TABLE execution_positions" in str(db.executed[0])
    assert "IN EXCLUSIVE MODE" in str(db.executed[0])
    assert "NOWAIT" in str(db.executed[0])


@pytest.mark.parametrize("acquired", [False, None])
def test_revocation_serialization_fails_closed_when_scope_is_busy(
    acquired: object,
) -> None:
    db = _LockSession(dialect="postgresql", acquired=acquired)

    with pytest.raises(
        core_revocation_exposure.RevocationExposureSerializationBusy
    ):
        core_revocation_exposure._acquire_revocation_order_intent_serialization(
            db=db,  # type: ignore[arg-type]
            settings=Settings(runtime_mode="live"),
            identity=PositionIdentity(
                provider="kis",
                account_scope="opaque-account-scope",
                exchange="KRX",
            ),
        )


def test_sqlite_revocation_contends_on_shared_kis_scope_until_exit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'runtime.db'}",
        runtime_mode="mock",
        kis_account_number="12345678",
        account_drawdown_recovery_scope_secret="s" * 32,
    )
    identity = PositionIdentity(
        provider="kis",
        account_scope="opaque-account-scope",
        exchange="KRX",
    )
    monkeypatch.setattr(
        core_revocation_exposure,
        "_expected_identity",
        lambda **_kwargs: identity,
    )
    item = ExecutionPlanItem(
        id="entry",
        execution_plan_id="plan",
        plan_item_index=0,
        symbol="029460",
        side="buy",
        intent="next_open_buy",
        provider="kis",
        venue="KRX",
    )
    local_scope = order_intent_scope_key_for_account(
        provider="kis",
        runtime_mode="mock",
        account_scope=recovery_account_fingerprint(settings),
        market="KRW",
    )
    process_lock = order_intent_process_lock(local_scope)
    holder_ready = threading.Event()
    release_holder = threading.Event()
    acquired_while_active: list[bool] = []
    acquired_after_exit: list[bool] = []

    def hold_lock() -> None:
        with process_lock:
            holder_ready.set()
            release_holder.wait(timeout=2)

    holder = threading.Thread(target=hold_lock)
    holder.start()
    assert holder_ready.wait(timeout=2)
    with pytest.raises(
        core_revocation_exposure.RevocationExposureSerializationBusy
    ):
        with core_revocation_exposure.serialized_revocation_exposure(
            db=_LockSession(dialect="sqlite"),  # type: ignore[arg-type]
            settings=settings,
            items=[item],
        ):
            pytest.fail("busy revocation serialization scope was acquired")
    release_holder.set()
    holder.join(timeout=2)
    assert not holder.is_alive()

    def try_lock(result: list[bool]) -> None:
        acquired = process_lock.acquire(blocking=False)
        result.append(acquired)
        if acquired:
            process_lock.release()

    db = _LockSession(dialect="sqlite")
    with core_revocation_exposure.serialized_revocation_exposure(
        db=db,  # type: ignore[arg-type]
        settings=settings,
        items=[item],
    ):
        contender = threading.Thread(
            target=try_lock,
            args=(acquired_while_active,),
        )
        contender.start()
        contender.join(timeout=2)

    contender = threading.Thread(
        target=try_lock,
        args=(acquired_after_exit,),
    )
    contender.start()
    contender.join(timeout=2)

    assert acquired_while_active == [False]
    assert acquired_after_exit == [True]
    assert db.driver_sql == ["BEGIN IMMEDIATE"]


def test_file_sqlite_revocation_fails_closed_on_shared_file_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    settings = Settings(
        database_url=f"sqlite:///{tmp_path / 'runtime.db'}",
        runtime_mode="mock",
        kis_account_number="12345678",
        account_drawdown_recovery_scope_secret="s" * 32,
    )
    identity = PositionIdentity(
        provider="kis",
        account_scope="opaque-account-scope",
        exchange="KRX",
    )
    monkeypatch.setattr(
        core_revocation_exposure,
        "_expected_identity",
        lambda **_kwargs: identity,
    )
    item = ExecutionPlanItem(
        id="entry",
        execution_plan_id="plan",
        plan_item_index=0,
        symbol="029460",
        side="buy",
        intent="next_open_buy",
        provider="kis",
        venue="KRX",
    )
    local_scope = order_intent_scope_key_for_account(
        provider="kis",
        runtime_mode="mock",
        account_scope=recovery_account_fingerprint(settings),
        market="KRW",
    )

    with order_intent_file_lock(local_scope):
        with pytest.raises(
            core_revocation_exposure.RevocationExposureSerializationBusy
        ):
            with core_revocation_exposure.serialized_revocation_exposure(
                db=_LockSession(dialect="sqlite"),  # type: ignore[arg-type]
                settings=settings,
                items=[item],
            ):
                pytest.fail("busy file-backed revocation scope was acquired")
