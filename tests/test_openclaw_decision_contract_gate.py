from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from app.core.config import Settings
from app.services import trigger_time_exec_agent
from app.services import openclaw_runtime_contract as runtime_contract
from app.services import watch_exec_agent_sizing as watch_sizing
from app.services.json_compaction import json_payload_hash
from app.services.openclaw_runtime_contract import (
    RuntimeContractSnapshot,
    RuntimeContractVerificationError,
    capture_runtime_contract,
    runtime_contract_proof_is_valid,
    stable_runtime_contract_proof,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = (
    REPO_ROOT
    / "runtime_contracts"
    / "openclaw"
    / "v1"
    / "exec-agent"
    / "AGENTS.md"
)


def _issued_proof_is_valid(value: object, *, consume: bool = False) -> bool:
    payload = value if isinstance(value, dict) else {}
    return runtime_contract_proof_is_valid(
        value,
        agent_id=str(payload.get("agent_id") or "exec-agent"),
        expected_model_ref=str(
            payload.get("expected_model_ref") or "openai/gpt-5.6-sol"
        ),
        requested_session_key=str(
            payload.get("requested_session_key") or "requested-contract-session"
        ),
        reported_session_id=str(
            payload.get("reported_session_id") or "reported-contract-session"
        ),
        request_sha256=str(payload.get("request_sha256") or "a" * 64),
        consume=consume,
    )


def _workspace(tmp_path: Path) -> tuple[Path, Path]:
    state_dir = tmp_path / ".openclaw"
    workspace = state_dir / "workspace-exec-agent"
    workspace.mkdir(parents=True)
    return state_dir, workspace / "AGENTS.md"


def test_runtime_contract_capture_honors_state_dir_and_matches(tmp_path, monkeypatch) -> None:
    state_dir, target = _workspace(tmp_path)
    content = TEMPLATE.read_bytes()
    target.write_bytes(content)
    monkeypatch.setenv("OPENCLAW_STATE_DIR", str(state_dir))

    snapshot = capture_runtime_contract(agent_id="exec-agent")

    digest = hashlib.sha256(content).hexdigest()
    target_stat = target.stat()
    assert snapshot.version == "v1"
    assert snapshot.expected_sha256 == digest
    assert snapshot.actual_sha256 == digest
    assert snapshot.status == "match"
    assert snapshot.file_fingerprint == (
        target_stat.st_dev,
        target_stat.st_ino,
        target_stat.st_size,
        target_stat.st_mtime_ns,
        target_stat.st_ctime_ns,
    )
    assert snapshot.directory_chain_fingerprint is not None
    assert snapshot.directory_chain_fingerprint[-1][0:2] == (
        target.parent.stat().st_dev,
        target.parent.stat().st_ino,
    )


@pytest.mark.parametrize(
    ("setup", "expected_status"),
    [
        ("missing", "missing"),
        ("mismatch", "mismatch"),
        ("symlink", "unsafe_target"),
        ("nonregular", "unsafe_target"),
    ],
)
def test_runtime_contract_capture_fails_closed_without_exposing_content(
    tmp_path,
    monkeypatch,
    setup: str,
    expected_status: str,
) -> None:
    state_dir, target = _workspace(tmp_path)
    sensitive_content = b"never-echo-runtime-contract-content\n"
    if setup == "mismatch":
        target.write_bytes(sensitive_content)
    elif setup == "symlink":
        outside = tmp_path / "outside.md"
        outside.write_bytes(sensitive_content)
        target.symlink_to(outside)
    elif setup == "nonregular":
        target.mkdir()
    monkeypatch.setenv("OPENCLAW_STATE_DIR", str(state_dir))

    snapshot = capture_runtime_contract(agent_id="exec-agent")

    assert snapshot.status == expected_status
    assert snapshot.expected_sha256 == hashlib.sha256(TEMPLATE.read_bytes()).hexdigest()
    if setup == "mismatch":
        assert snapshot.actual_sha256 == hashlib.sha256(sensitive_content).hexdigest()
    else:
        assert snapshot.actual_sha256 is None
    assert sensitive_content.decode().strip() not in str(snapshot)


def test_runtime_contract_capture_fails_closed_on_io_error(tmp_path, monkeypatch) -> None:
    state_dir, target = _workspace(tmp_path)
    target.write_bytes(TEMPLATE.read_bytes())
    monkeypatch.setenv("OPENCLAW_STATE_DIR", str(state_dir))
    real_open = os.open

    def fail_target_open(path, *args, **kwargs):
        if path == "AGENTS.md":
            raise OSError("simulated read failure")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr("app.services.openclaw_runtime_contract.os.open", fail_target_open)

    snapshot = capture_runtime_contract(agent_id="exec-agent")

    assert snapshot.status == "io_error"
    assert snapshot.actual_sha256 is None


@pytest.mark.parametrize("selector", ["", "relative/state", "/tmp/state/../other"])
def test_runtime_contract_rejects_unsafe_state_directory_selector(
    selector: str,
    monkeypatch,
) -> None:
    monkeypatch.setenv("OPENCLAW_STATE_DIR", selector)

    snapshot = capture_runtime_contract(agent_id="exec-agent")

    assert snapshot.status == "unsafe_state_dir"


def test_runtime_contract_rejects_symlinked_default_state_directory(
    tmp_path,
    monkeypatch,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    actual_state, target = _workspace(tmp_path / "outside")
    target.write_bytes(TEMPLATE.read_bytes())
    (home / ".openclaw").symlink_to(actual_state, target_is_directory=True)
    monkeypatch.delenv("OPENCLAW_STATE_DIR", raising=False)
    monkeypatch.setenv("HOME", str(home))

    snapshot = capture_runtime_contract(agent_id="exec-agent")

    assert snapshot.status == "unsafe_state_dir"


def test_runtime_contract_rejects_state_directory_with_symlink_component(
    tmp_path,
    monkeypatch,
) -> None:
    real_parent = tmp_path / "real-parent"
    state_dir, target = _workspace(real_parent)
    target.write_bytes(TEMPLATE.read_bytes())
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(real_parent, target_is_directory=True)
    monkeypatch.setenv(
        "OPENCLAW_STATE_DIR",
        str(linked_parent / state_dir.relative_to(real_parent)),
    )

    snapshot = capture_runtime_contract(agent_id="exec-agent")

    assert snapshot.status == "unsafe_state_dir"


def test_runtime_contract_workspace_io_error_fails_closed(
    tmp_path,
    monkeypatch,
) -> None:
    state_dir, target = _workspace(tmp_path)
    target.write_bytes(TEMPLATE.read_bytes())
    monkeypatch.setenv("OPENCLAW_STATE_DIR", str(state_dir))
    real_stat = os.stat

    def fail_workspace_stat(path, *args, **kwargs):
        if path == "workspace-exec-agent":
            raise PermissionError("simulated workspace read failure")
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr("app.services.openclaw_runtime_contract.os.stat", fail_workspace_stat)

    snapshot = capture_runtime_contract(agent_id="exec-agent")

    assert snapshot.status == "io_error"


@pytest.mark.parametrize("scope", ["state", "workspace"])
def test_runtime_contract_fstat_io_error_closes_directory_descriptor(
    scope: str,
    tmp_path,
    monkeypatch,
) -> None:
    state_dir, target = _workspace(tmp_path)
    target.write_bytes(TEMPLATE.read_bytes())
    monkeypatch.setenv("OPENCLAW_STATE_DIR", str(state_dir))
    monkeypatch.setattr(
        runtime_contract,
        "_read_expected_contract",
        lambda _path: TEMPLATE.read_bytes(),
    )
    real_open = os.open
    real_fstat = os.fstat
    failed_descriptor: list[int] = []

    def tracking_open(path, flags, *args, **kwargs):
        descriptor = real_open(path, flags, *args, **kwargs)
        if scope == "state" and path == state_dir.anchor and not failed_descriptor:
            failed_descriptor.append(descriptor)
        elif scope == "workspace" and path == "workspace-exec-agent":
            failed_descriptor.append(descriptor)
        return descriptor

    def fail_selected_fstat(descriptor: int):
        if failed_descriptor and descriptor == failed_descriptor[-1]:
            raise PermissionError("simulated directory fstat failure")
        return real_fstat(descriptor)

    monkeypatch.setattr(runtime_contract.os, "open", tracking_open)
    monkeypatch.setattr(runtime_contract.os, "fstat", fail_selected_fstat)

    snapshot = capture_runtime_contract(agent_id="exec-agent")

    assert snapshot.status == "io_error"
    assert failed_descriptor
    with pytest.raises(OSError):
        real_fstat(failed_descriptor[-1])


def test_runtime_contract_proof_requires_lowercase_sha256_hex() -> None:
    proof = {
        "version": "v1",
        "expected_sha256": "z" * 64,
        "actual_sha256": "z" * 64,
        "pre_run_actual_sha256": "z" * 64,
        "post_run_actual_sha256": "z" * 64,
        "status": "match",
        "stable_across_run": True,
    }

    assert _issued_proof_is_valid(proof) is False


def test_runtime_contract_proof_rejects_structurally_valid_external_forgery() -> None:
    digest = hashlib.sha256(TEMPLATE.read_bytes()).hexdigest()
    forged_proof = {
        "version": "v1",
        "expected_sha256": digest,
        "actual_sha256": digest,
        "pre_run_actual_sha256": digest,
        "post_run_actual_sha256": digest,
        "status": "match",
        "stable_across_run": True,
    }

    assert _issued_proof_is_valid(forged_proof) is False


def test_runtime_contract_proof_rejects_serialized_replay() -> None:
    issued_proof = _bound_proof(
        settings=Settings(),
        request_payload={"plan_id": "serialized-proof"},
    )
    replayed_proof = json.loads(json.dumps(issued_proof))

    assert _issued_proof_is_valid(issued_proof) is True
    assert _issued_proof_is_valid(replayed_proof) is False


@pytest.mark.parametrize(
    "clone_proof",
    [
        copy.copy,
        copy.deepcopy,
        lambda proof: type(proof)(dict(proof)),
    ],
    ids=["shallow-copy", "deep-copy", "reconstructed-issued-type"],
)
def test_runtime_contract_proof_clone_cannot_replay_after_consumption(
    clone_proof,
) -> None:
    issued_proof = _bound_proof(
        settings=Settings(),
        request_payload={"plan_id": "cloned-proof"},
    )
    cloned_proof = clone_proof(issued_proof)

    assert _issued_proof_is_valid(issued_proof, consume=True) is True
    assert _issued_proof_is_valid(cloned_proof, consume=True) is False


def test_runtime_contract_proof_concurrent_copies_have_one_consumer() -> None:
    issued_proof = _bound_proof(
        settings=Settings(),
        request_payload={"plan_id": "concurrent-proof"},
    )
    proof_copies = [copy.copy(issued_proof), copy.copy(issued_proof)]
    start = threading.Barrier(len(proof_copies))

    def consume(proof: object) -> bool:
        start.wait()
        return _issued_proof_is_valid(proof, consume=True)

    with ThreadPoolExecutor(max_workers=len(proof_copies)) as executor:
        results = list(executor.map(consume, proof_copies))

    assert sorted(results) == [False, True]


def test_runtime_contract_proof_expires(monkeypatch) -> None:
    clock = [100.0]
    monkeypatch.setattr(runtime_contract, "_PROOF_REGISTRY_TTL_SECONDS", 1.0, raising=False)
    monkeypatch.setattr(
        runtime_contract,
        "_proof_registry_now",
        lambda: clock[0],
        raising=False,
    )
    issued_proof = _bound_proof(
        settings=Settings(),
        request_payload={"plan_id": "expiring-proof"},
    )

    clock[0] += 2.0

    assert _issued_proof_is_valid(issued_proof, consume=True) is False


def test_runtime_contract_proof_samples_expiry_inside_consumption_lock(
    monkeypatch,
) -> None:
    issued_proof = _bound_proof(
        settings=Settings(),
        request_payload={"plan_id": "lock-expiring-proof"},
    )
    nonce = str(issued_proof["nonce"])
    expires_at = runtime_contract._PROOF_REGISTRY[nonce][1]
    clock = [expires_at - 1.0]

    class ExpireWhenLocked:
        def __enter__(self):
            clock[0] = expires_at + 1.0

        def __exit__(self, _exc_type, _exc, _traceback):
            return False

    monkeypatch.setattr(runtime_contract, "_proof_registry_now", lambda: clock[0])
    monkeypatch.setattr(
        runtime_contract,
        "_PROOF_CONSUMPTION_LOCK",
        ExpireWhenLocked(),
    )

    assert _issued_proof_is_valid(issued_proof, consume=True) is False


def test_runtime_contract_proof_registry_backpressures_without_evicting_live_proofs(
    monkeypatch,
) -> None:
    registry = type(runtime_contract._PROOF_REGISTRY)()
    monkeypatch.setattr(runtime_contract, "_PROOF_REGISTRY", registry)
    monkeypatch.setattr(
        runtime_contract,
        "_PROOF_REGISTRY_MAX_ENTRIES",
        2,
        raising=False,
    )
    proofs = [
        _bound_proof(
            settings=Settings(),
            request_payload={"plan_id": f"bounded-proof-{index}"},
        )
        for index in range(2)
    ]

    with pytest.raises(
        RuntimeContractVerificationError,
        match="proof issuance failed",
    ):
        _bound_proof(
            settings=Settings(),
            request_payload={"plan_id": "bounded-proof-overflow"},
        )

    assert set(registry) == {str(proof["nonce"]) for proof in proofs}
    assert all(_issued_proof_is_valid(proof, consume=True) for proof in proofs)


def test_runtime_contract_proof_registry_reclaims_consumed_entry_before_backpressure(
    monkeypatch,
) -> None:
    registry = type(runtime_contract._PROOF_REGISTRY)()
    monkeypatch.setattr(runtime_contract, "_PROOF_REGISTRY", registry)
    monkeypatch.setattr(runtime_contract, "_PROOF_REGISTRY_MAX_ENTRIES", 1)
    consumed_proof = _bound_proof(
        settings=Settings(),
        request_payload={"plan_id": "consumed-capacity-proof"},
    )

    assert _issued_proof_is_valid(consumed_proof, consume=True) is True

    replacement_proof = _bound_proof(
        settings=Settings(),
        request_payload={"plan_id": "replacement-capacity-proof"},
    )

    assert set(registry) == {str(replacement_proof["nonce"])}
    assert _issued_proof_is_valid(replacement_proof, consume=True) is True


def test_runtime_contract_proof_registry_reclaims_expired_entry_before_backpressure(
    monkeypatch,
) -> None:
    registry = type(runtime_contract._PROOF_REGISTRY)()
    clock = [100.0]
    monkeypatch.setattr(runtime_contract, "_PROOF_REGISTRY", registry)
    monkeypatch.setattr(runtime_contract, "_PROOF_REGISTRY_MAX_ENTRIES", 1)
    monkeypatch.setattr(runtime_contract, "_PROOF_REGISTRY_TTL_SECONDS", 1.0)
    monkeypatch.setattr(runtime_contract, "_proof_registry_now", lambda: clock[0])
    expired_proof = _bound_proof(
        settings=Settings(),
        request_payload={"plan_id": "expired-capacity-proof"},
    )

    clock[0] += 2.0
    replacement_proof = _bound_proof(
        settings=Settings(),
        request_payload={"plan_id": "replacement-expired-capacity-proof"},
    )

    assert set(registry) == {str(replacement_proof["nonce"])}
    assert _issued_proof_is_valid(expired_proof, consume=True) is False
    assert _issued_proof_is_valid(replacement_proof, consume=True) is True


def test_runtime_contract_proof_nonce_collision_fails_without_replacing_live_proof(
    monkeypatch,
) -> None:
    registry = type(runtime_contract._PROOF_REGISTRY)()
    fixed_nonce = "c" * 32
    monkeypatch.setattr(runtime_contract, "_PROOF_REGISTRY", registry)
    monkeypatch.setattr(
        runtime_contract.secrets,
        "token_hex",
        lambda _size: fixed_nonce,
    )
    issued_proof = _bound_proof(
        settings=Settings(),
        request_payload={"plan_id": "nonce-collision-original"},
    )

    with pytest.raises(
        RuntimeContractVerificationError,
        match="proof issuance failed",
    ):
        _bound_proof(
            settings=Settings(),
            request_payload={"plan_id": "nonce-collision-replacement"},
        )

    assert list(registry) == [fixed_nonce]
    assert _issued_proof_is_valid(issued_proof, consume=True) is True


def test_runtime_contract_proof_rejects_unregistered_exact_type_with_valid_hmac(
    monkeypatch,
) -> None:
    registry = type(runtime_contract._PROOF_REGISTRY)()
    monkeypatch.setattr(runtime_contract, "_PROOF_REGISTRY", registry)
    issued_proof = _bound_proof(
        settings=Settings(),
        request_payload={"plan_id": "unregistered-exact-type-proof"},
    )
    registry.pop(str(issued_proof["nonce"]))

    assert type(issued_proof) is runtime_contract._IssuedRuntimeContractProof
    assert _issued_proof_is_valid(issued_proof, consume=True) is False


def test_runtime_contract_proof_consumption_invalidates_all_future_checks(
    monkeypatch,
) -> None:
    registry = type(runtime_contract._PROOF_REGISTRY)()
    monkeypatch.setattr(runtime_contract, "_PROOF_REGISTRY", registry)
    issued_proof = _bound_proof(
        settings=Settings(),
        request_payload={"plan_id": "consumed-proof-validity"},
    )

    assert _issued_proof_is_valid(issued_proof, consume=True) is True
    assert _issued_proof_is_valid(issued_proof, consume=False) is False
    assert _issued_proof_is_valid(issued_proof, consume=True) is False


def test_runtime_contract_proof_registry_concurrent_issuance_is_bounded(
    monkeypatch,
) -> None:
    registry = type(runtime_contract._PROOF_REGISTRY)()
    capacity = 4
    attempt_count = 12
    start = threading.Barrier(attempt_count)
    settings = Settings()
    monkeypatch.setattr(runtime_contract, "_PROOF_REGISTRY", registry)
    monkeypatch.setattr(runtime_contract, "_PROOF_REGISTRY_MAX_ENTRIES", capacity)

    def issue(index: int) -> dict[str, object] | None:
        start.wait()
        try:
            return _bound_proof(
                settings=settings,
                request_payload={
                    "plan_id": f"concurrent-capacity-proof-{index}"
                },
            )
        except RuntimeContractVerificationError:
            return None

    with ThreadPoolExecutor(max_workers=attempt_count) as executor:
        results = list(executor.map(issue, range(attempt_count)))

    issued_proofs = [proof for proof in results if proof is not None]
    assert len(issued_proofs) == capacity
    assert len(registry) == capacity
    assert set(registry) == {str(proof["nonce"]) for proof in issued_proofs}
    assert all(_issued_proof_is_valid(proof) for proof in issued_proofs)
    assert all(
        _issued_proof_is_valid(proof, consume=True) for proof in issued_proofs
    )


def test_runtime_contract_proof_rejects_change_then_restore_fingerprint() -> None:
    before = RuntimeContractSnapshot(
        version="v1",
        expected_sha256="a" * 64,
        actual_sha256="a" * 64,
        status="match",
        file_fingerprint=(1, 2, 100, 10, 20),
        directory_chain_fingerprint=((10, 11, 12, 13, 14),),
    )
    after = RuntimeContractSnapshot(
        version="v1",
        expected_sha256="a" * 64,
        actual_sha256="a" * 64,
        status="match",
        file_fingerprint=(1, 2, 100, 10, 21),
        directory_chain_fingerprint=((10, 11, 12, 13, 14),),
    )

    with pytest.raises(RuntimeContractVerificationError, match="changed_during_run"):
        stable_runtime_contract_proof(
            agent_id="exec-agent",
            expected_model_ref="openai/gpt-5.6-sol",
            requested_session_key="requested-contract-session",
            reported_session_id="reported-contract-session",
            request_sha256="a" * 64,
            before=before,
            after=after,
        )


def _snapshot(status: str = "match", *, digest: str = "a" * 64) -> RuntimeContractSnapshot:
    return RuntimeContractSnapshot(
        version="v1",
        expected_sha256="a" * 64,
        actual_sha256=digest if status in {"match", "mismatch"} else None,
        status=status,
        file_fingerprint=(1, 2, 3, 4, 5),
        directory_chain_fingerprint=(
            (10, 11, 12, 13, 14),
            (20, 21, 22, 23, 24),
        ),
    )


def _trigger_payload() -> dict[str, object]:
    return {
        "plan": {"plan_id": "plan-contract-gate"},
        "signal_context": {
            "execution_plan_item_id": "item-contract-gate",
            "symbol": "005930",
        },
    }


def _trigger_response() -> dict[str, object]:
    return {
        "version": "trigger-time-exec-agent-v1",
        "plan_id": "plan-contract-gate",
        "execution_plan_item_id": "item-contract-gate",
        "symbol": "005930",
        "decision": "execute_now",
        "side": "buy",
        "quantity": 1,
        "reason": "contract verified",
        "context_staleness_ok": True,
    }


def _runner_response(
    *,
    settings: Settings,
    decision: dict[str, object],
    include_agent_id: bool = True,
) -> subprocess.CompletedProcess[str]:
    provider, model = settings.exec_agent_model.split("/", 1)
    agent_meta = {
        "provider": provider,
        "model": model,
        "sessionId": "contract-gate-session",
    }
    if include_agent_id:
        agent_meta["agentId"] = settings.exec_agent_id
    return subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(
            {
                "meta": {
                    "agentMeta": agent_meta,
                },
                "payloads": [{"text": json.dumps(decision)}],
            }
        ),
        stderr="",
    )


def _bound_proof(
    *,
    settings: Settings,
    request_payload: dict[str, object],
    requested_session_key: str = "requested-contract-session",
    reported_session_id: str = "reported-contract-session",
) -> dict[str, object]:
    snapshot = _snapshot()
    return stable_runtime_contract_proof(
        agent_id=settings.exec_agent_id,
        expected_model_ref=settings.exec_agent_model,
        requested_session_key=requested_session_key,
        reported_session_id=reported_session_id,
        request_sha256=json_payload_hash(request_payload),
        before=snapshot,
        after=snapshot,
    )


def _bound_provenance(
    *,
    settings: Settings,
    request_payload: dict[str, object],
) -> dict[str, object]:
    requested_session_key = "requested-contract-session"
    reported_session_id = "reported-contract-session"
    return {
        "actual_agent_id": settings.exec_agent_id,
        "actual_model_ref": settings.exec_agent_model,
        "session_id": reported_session_id,
        "requested_session_key": requested_session_key,
        "request_sha256": json_payload_hash(request_payload),
        "runtime_contract": _bound_proof(
            settings=settings,
            request_payload=request_payload,
            requested_session_key=requested_session_key,
            reported_session_id=reported_session_id,
        ),
    }


@pytest.mark.parametrize("path", ["trigger", "watch"])
@pytest.mark.parametrize(
    "transplant",
    ["request", "requested_session", "reported_session", "model"],
)
def test_decision_verifier_rejects_bound_proof_transplants(
    path: str,
    transplant: str,
) -> None:
    settings = Settings()
    request_payload = {"plan_id": "bound-plan", "candidates": []}
    provenance = _bound_provenance(
        settings=settings,
        request_payload=request_payload,
    )
    verified_settings = settings
    verified_request = request_payload
    if transplant == "request":
        verified_request = {"plan_id": "different-plan", "candidates": []}
    elif transplant == "requested_session":
        provenance["requested_session_key"] = "different-requested-session"
    elif transplant == "reported_session":
        provenance["session_id"] = "different-reported-session"
    else:
        verified_settings = Settings(exec_agent_model="openai/gpt-5.6-terra")
    verifier = (
        trigger_time_exec_agent._verified_runtime_provenance
        if path == "trigger"
        else watch_sizing._verified_runtime_provenance
    )

    assert verifier(
        settings=verified_settings,
        provenance=provenance,
        request_payload=verified_request,
    ) is False


@pytest.mark.parametrize("path", ["trigger", "watch"])
def test_decision_verifier_consumes_bound_proof_once(path: str) -> None:
    settings = Settings()
    request_payload = {"plan_id": "single-use-plan", "candidates": []}
    provenance = _bound_provenance(
        settings=settings,
        request_payload=request_payload,
    )
    verifier = (
        trigger_time_exec_agent._verified_runtime_provenance
        if path == "trigger"
        else watch_sizing._verified_runtime_provenance
    )

    assert verifier(
        settings=settings,
        provenance=provenance,
        request_payload=request_payload,
    ) is True
    assert verifier(
        settings=settings,
        provenance=provenance,
        request_payload=request_payload,
    ) is False


@pytest.mark.parametrize("path", ["trigger", "watch"])
def test_decision_paths_attach_stable_contract_proof(path: str, monkeypatch) -> None:
    settings = Settings(runtime_mode="paper")
    snapshots = iter([_snapshot(), _snapshot()])
    runner_calls = 0

    def capture(**_kwargs):
        return next(snapshots)

    def run(*_args, **_kwargs):
        nonlocal runner_calls
        runner_calls += 1
        decision = (
            _trigger_response()
            if path == "trigger"
            else {
                "version": "manual-exec-agent-watch-sizing-v1",
                "plan_id": "plan-contract-gate",
                "decisions": [],
            }
        )
        decision["_runtime_provenance"] = {
            "runtime_contract": {"status": "forged"}
        }
        decision["_openclaw_runtime_provenance"] = {
            "runtime_contract": {"status": "forged"}
        }
        return _runner_response(settings=settings, decision=decision)

    module = trigger_time_exec_agent if path == "trigger" else watch_sizing
    monkeypatch.setattr(module, "capture_runtime_contract", capture)
    monkeypatch.setattr(module, "_run_exec_agent_command", run)

    if path == "trigger":
        response = trigger_time_exec_agent._request_exec_agent_decision(
            settings=settings,
            payload=_trigger_payload(),
        )
        provenance = response["_openclaw_runtime_provenance"]
    else:
        response = watch_sizing._request_exec_agent_decisions(
            settings=settings,
            payload={"plan_id": "plan-contract-gate", "candidates": []},
        )
        provenance = response["_runtime_provenance"]

    assert runner_calls == 1
    contract_proof = provenance["runtime_contract"]
    assert isinstance(contract_proof, dict)
    assert contract_proof["agent_id"] == "exec-agent"
    assert contract_proof["version"] == "v1"
    assert contract_proof["expected_sha256"] == "a" * 64
    assert contract_proof["actual_sha256"] == "a" * 64
    assert contract_proof["pre_run_actual_sha256"] == "a" * 64
    assert contract_proof["post_run_actual_sha256"] == "a" * 64
    assert contract_proof["status"] == "match"
    assert contract_proof["stable_across_run"] is True
    assert _issued_proof_is_valid(contract_proof)


@pytest.mark.parametrize("path", ["trigger", "watch"])
def test_current_openclaw_schema_binds_missing_agent_id_only_from_signed_contract(
    path: str,
    monkeypatch,
) -> None:
    settings = Settings(runtime_mode="paper")
    snapshots = iter([_snapshot(), _snapshot()])
    request_payload = (
        _trigger_payload()
        if path == "trigger"
        else {"plan_id": "plan-contract-gate", "candidates": []}
    )
    decision = (
        _trigger_response()
        if path == "trigger"
        else {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": "plan-contract-gate",
            "decisions": [],
        }
    )
    module = trigger_time_exec_agent if path == "trigger" else watch_sizing
    monkeypatch.setattr(module, "capture_runtime_contract", lambda **_: next(snapshots))
    monkeypatch.setattr(
        module,
        "_run_exec_agent_command",
        lambda *_args, **_kwargs: _runner_response(
            settings=settings,
            decision=decision,
            include_agent_id=False,
        ),
    )

    if path == "trigger":
        response = trigger_time_exec_agent._request_exec_agent_decision(
            settings=settings,
            payload=request_payload,
        )
        provenance = response["_openclaw_runtime_provenance"]
        verifier = trigger_time_exec_agent._verified_runtime_provenance
    else:
        response = watch_sizing._request_exec_agent_decisions(
            settings=settings,
            payload=request_payload,
        )
        provenance = response["_runtime_provenance"]
        verifier = watch_sizing._verified_runtime_provenance

    assert provenance["actual_agent_id"] is None
    assert provenance["verified_agent_id"] == settings.exec_agent_id
    assert provenance["agent_identity_source"] == "signed_runtime_contract"
    assert verifier(
        settings=settings,
        provenance=provenance,
        request_payload=request_payload,
    )


@pytest.mark.parametrize("path", ["trigger", "watch"])
def test_reported_wrong_agent_id_cannot_be_overridden_by_signed_identity_fallback(
    path: str,
) -> None:
    settings = Settings()
    request_payload = {"plan_id": "reported-agent-mismatch", "candidates": []}
    provenance = _bound_provenance(
        settings=settings,
        request_payload=request_payload,
    )
    provenance.update(
        {
            "actual_agent_id": "wrong-agent",
            "verified_agent_id": settings.exec_agent_id,
            "agent_identity_source": "signed_runtime_contract",
        }
    )
    verifier = (
        trigger_time_exec_agent._verified_runtime_provenance
        if path == "trigger"
        else watch_sizing._verified_runtime_provenance
    )

    assert verifier(
        settings=settings,
        provenance=provenance,
        request_payload=request_payload,
    ) is False


@pytest.mark.parametrize("path", ["trigger", "watch"])
@pytest.mark.parametrize(
    "malformed_agent_id",
    [None, 0, {"unexpected": "object"}],
)
def test_present_malformed_agent_id_cannot_use_signed_identity_fallback(
    path: str,
    malformed_agent_id: object,
    monkeypatch,
) -> None:
    settings = Settings(runtime_mode="paper")
    snapshots = iter([_snapshot(), _snapshot()])
    request_payload = (
        _trigger_payload()
        if path == "trigger"
        else {"plan_id": "plan-contract-gate", "candidates": []}
    )
    decision = (
        _trigger_response()
        if path == "trigger"
        else {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": "plan-contract-gate",
            "decisions": [],
        }
    )
    module = trigger_time_exec_agent if path == "trigger" else watch_sizing
    monkeypatch.setattr(module, "capture_runtime_contract", lambda **_: next(snapshots))

    def malformed_runner(*_args, **_kwargs):
        completed = _runner_response(
            settings=settings,
            decision=decision,
            include_agent_id=False,
        )
        envelope = json.loads(completed.stdout)
        envelope["meta"]["agentMeta"]["agentId"] = malformed_agent_id
        return subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps(envelope),
            stderr="",
        )

    monkeypatch.setattr(module, "_run_exec_agent_command", malformed_runner)

    if path == "trigger":
        response = trigger_time_exec_agent._request_exec_agent_decision(
            settings=settings,
            payload=request_payload,
        )
        provenance = response["_openclaw_runtime_provenance"]
        verifier = trigger_time_exec_agent._verified_runtime_provenance
    else:
        response = watch_sizing._request_exec_agent_decisions(
            settings=settings,
            payload=request_payload,
        )
        provenance = response["_runtime_provenance"]
        verifier = watch_sizing._verified_runtime_provenance

    assert "verified_agent_id" not in provenance
    assert "agent_identity_source" not in provenance
    assert verifier(
        settings=settings,
        provenance=provenance,
        request_payload=request_payload,
    ) is False


@pytest.mark.parametrize("path", ["trigger", "watch"])
def test_decision_verifier_requires_non_forgeable_contract_proof(path: str) -> None:
    settings = Settings()
    request_payload = {"plan_id": "proof-required", "candidates": []}
    base = _bound_provenance(
        settings=settings,
        request_payload=request_payload,
    )
    verifier = (
        trigger_time_exec_agent._verified_runtime_provenance
        if path == "trigger"
        else watch_sizing._verified_runtime_provenance
    )

    assert verifier(
        settings=settings,
        provenance=base,
        request_payload=request_payload,
    )
    missing_proof = dict(base)
    missing_proof.pop("runtime_contract")
    assert verifier(
        settings=settings,
        provenance=missing_proof,
        request_payload=request_payload,
    ) is False
    assert verifier(
        settings=settings,
        provenance={
            **base,
            "runtime_contract": {
                "version": "v1",
                "expected_sha256": "z" * 64,
                "actual_sha256": "z" * 64,
                "pre_run_actual_sha256": "z" * 64,
                "post_run_actual_sha256": "z" * 64,
                "status": "match",
                "stable_across_run": True,
            },
        },
        request_payload=request_payload,
    ) is False


@pytest.mark.parametrize("path", ["trigger", "watch"])
@pytest.mark.parametrize(
    "status",
    ["missing", "mismatch", "unsafe_target", "unsafe_workspace", "io_error"],
)
def test_decision_paths_reject_unverified_contract_before_agent_run(
    path: str,
    status: str,
    monkeypatch,
) -> None:
    settings = Settings(runtime_mode="paper")
    module = trigger_time_exec_agent if path == "trigger" else watch_sizing
    monkeypatch.setattr(module, "capture_runtime_contract", lambda **_: _snapshot(status))
    monkeypatch.setattr(
        module,
        "_run_exec_agent_command",
        lambda *_args, **_kwargs: pytest.fail("agent command must not run"),
    )

    with pytest.raises(RuntimeContractVerificationError, match=status):
        if path == "trigger":
            trigger_time_exec_agent._request_exec_agent_decision(
                settings=settings,
                payload=_trigger_payload(),
            )
        else:
            watch_sizing._request_exec_agent_decisions(
                settings=settings,
                payload={"plan_id": "plan-contract-gate", "candidates": []},
            )


@pytest.mark.parametrize("path", ["trigger", "watch"])
def test_decision_paths_reject_contract_drift_during_agent_run(
    path: str,
    monkeypatch,
) -> None:
    settings = Settings(runtime_mode="paper")
    snapshots = iter([_snapshot(), _snapshot("mismatch", digest="b" * 64)])
    module = trigger_time_exec_agent if path == "trigger" else watch_sizing
    monkeypatch.setattr(module, "capture_runtime_contract", lambda **_: next(snapshots))
    decision = (
        _trigger_response()
        if path == "trigger"
        else {
            "version": "manual-exec-agent-watch-sizing-v1",
            "plan_id": "plan-contract-gate",
            "decisions": [],
        }
    )
    monkeypatch.setattr(
        module,
        "_run_exec_agent_command",
        lambda *_args, **_kwargs: _runner_response(
            settings=settings,
            decision=decision,
        ),
    )

    with pytest.raises(RuntimeContractVerificationError, match="changed_during_run"):
        if path == "trigger":
            trigger_time_exec_agent._request_exec_agent_decision(
                settings=settings,
                payload=_trigger_payload(),
            )
        else:
            watch_sizing._request_exec_agent_decisions(
                settings=settings,
                payload={"plan_id": "plan-contract-gate", "candidates": []},
            )


@pytest.mark.parametrize("path", ["trigger", "watch"])
def test_decision_paths_reject_state_dir_swap_and_restore_during_agent_run(
    path: str,
    tmp_path,
    monkeypatch,
) -> None:
    state_dir, target = _workspace(tmp_path)
    target.write_bytes(TEMPLATE.read_bytes())
    malicious_state, malicious_target = _workspace(tmp_path / "malicious")
    malicious_target.write_text("malicious contract\n", encoding="utf-8")
    saved_state = tmp_path / "saved-state"
    monkeypatch.setenv("OPENCLAW_STATE_DIR", str(state_dir))
    baseline = capture_runtime_contract(agent_id="exec-agent")
    settings = Settings(runtime_mode="paper")
    module = trigger_time_exec_agent if path == "trigger" else watch_sizing

    def swap_run(*_args, **_kwargs):
        state_dir.rename(saved_state)
        state_dir.symlink_to(malicious_state, target_is_directory=True)
        try:
            decision = (
                _trigger_response()
                if path == "trigger"
                else {
                    "version": "manual-exec-agent-watch-sizing-v1",
                    "plan_id": "plan-contract-gate",
                    "decisions": [],
                }
            )
            return _runner_response(settings=settings, decision=decision)
        finally:
            state_dir.unlink()
            saved_state.rename(state_dir)

    monkeypatch.setattr(module, "_run_exec_agent_command", swap_run)

    with pytest.raises(RuntimeContractVerificationError, match="changed_during_run"):
        if path == "trigger":
            trigger_time_exec_agent._request_exec_agent_decision(
                settings=settings,
                payload=_trigger_payload(),
            )
        else:
            watch_sizing._request_exec_agent_decisions(
                settings=settings,
                payload={"plan_id": "plan-contract-gate", "candidates": []},
            )

    restored = capture_runtime_contract(agent_id="exec-agent")
    assert restored.file_fingerprint == baseline.file_fingerprint
    assert restored.directory_chain_fingerprint != baseline.directory_chain_fingerprint
