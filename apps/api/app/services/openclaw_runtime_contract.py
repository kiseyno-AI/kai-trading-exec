from __future__ import annotations

import errno
import hashlib
import hmac
import os
import re
import secrets
import stat
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path


RUNTIME_CONTRACT_VERSION = "v1"
_REPO_ROOT = Path(__file__).resolve().parents[4]
_EXPECTED_CONTRACTS = {
    "exec-agent": (
        _REPO_ROOT
        / "runtime_contracts"
        / "openclaw"
        / RUNTIME_CONTRACT_VERSION
        / "exec-agent"
        / "AGENTS.md"
    ),
}
_WORKSPACE_NAMES = {
    "exec-agent": "workspace-exec-agent",
}
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_NONCE_PATTERN = re.compile(r"[0-9a-f]{32}")
_PROOF_ATTESTATION_KEY = secrets.token_bytes(32)
_PROOF_CONSUMPTION_LOCK = threading.Lock()
_PROOF_REGISTRY_TTL_SECONDS = 300.0
_PROOF_REGISTRY_MAX_ENTRIES = 4096
_PROOF_REGISTRY: OrderedDict[str, tuple[str, float, bool]] = OrderedDict()
FileFingerprint = tuple[int, int, int, int, int]
DirectoryChainFingerprint = tuple[FileFingerprint, ...]


@dataclass(frozen=True)
class RuntimeContractSnapshot:
    version: str
    expected_sha256: str
    actual_sha256: str | None
    status: str
    file_fingerprint: FileFingerprint | None = None
    directory_chain_fingerprint: DirectoryChainFingerprint | None = None

    @property
    def matches(self) -> bool:
        return bool(
            self.status == "match"
            and self.actual_sha256
            and self.actual_sha256 == self.expected_sha256
        )


class RuntimeContractVerificationError(RuntimeError):
    """A content-free failure raised when the live agent contract is untrusted."""


class _IssuedRuntimeContractProof(dict[str, object]):
    """Process-local proof type that cannot be reconstructed from agent JSON."""

    __slots__ = ()

    def __init__(self, value: dict[str, object]) -> None:
        super().__init__(value)


class _UnsafeTargetError(Exception):
    pass


class _UnsafeWorkspaceError(Exception):
    pass


class _WorkspaceIOError(Exception):
    pass


class _UnsafeStateDirectoryError(Exception):
    pass


class _StateDirectoryIOError(Exception):
    pass


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _proof_registry_now() -> float:
    return time.monotonic()


def _purge_proof_registry_locked(now: float) -> None:
    stale = [
        nonce
        for nonce, (_attestation, expires_at, consumed) in _PROOF_REGISTRY.items()
        if consumed or expires_at <= now
    ]
    for nonce in stale:
        _PROOF_REGISTRY.pop(nonce, None)


def _register_issued_proof(*, nonce: str, attestation: str) -> None:
    with _PROOF_CONSUMPTION_LOCK:
        now = _proof_registry_now()
        _purge_proof_registry_locked(now)
        if nonce in _PROOF_REGISTRY:
            raise RuntimeContractVerificationError(
                "OpenClaw runtime contract proof issuance failed"
            )
        if _PROOF_REGISTRY_TTL_SECONDS <= 0 or _PROOF_REGISTRY_MAX_ENTRIES <= 0:
            raise RuntimeContractVerificationError(
                "OpenClaw runtime contract proof issuance failed"
            )
        if len(_PROOF_REGISTRY) >= _PROOF_REGISTRY_MAX_ENTRIES:
            raise RuntimeContractVerificationError(
                "OpenClaw runtime contract proof issuance failed"
            )
        _PROOF_REGISTRY[nonce] = (
            attestation,
            now + _PROOF_REGISTRY_TTL_SECONDS,
            False,
        )


def _registered_proof_is_valid(
    *,
    nonce: str,
    attestation: str,
    consume: bool,
) -> bool:
    with _PROOF_CONSUMPTION_LOCK:
        now = _proof_registry_now()
        _purge_proof_registry_locked(now)
        entry = _PROOF_REGISTRY.get(nonce)
        if entry is None:
            return False
        registered_attestation, expires_at, consumed = entry
        if (
            consumed
            or expires_at <= now
            or not hmac.compare_digest(registered_attestation, attestation)
        ):
            return False
        if consume:
            _PROOF_REGISTRY[nonce] = (
                registered_attestation,
                expires_at,
                True,
            )
        return True


def _proof_attestation(
    *,
    agent_id: str,
    expected_model_ref: str,
    requested_session_key: str,
    reported_session_id: str,
    request_sha256: str,
    nonce: str,
    version: str,
    expected_sha256: str,
    actual_sha256: str,
    pre_run_actual_sha256: str,
    post_run_actual_sha256: str,
    pre_run_file_fingerprint: FileFingerprint,
    post_run_file_fingerprint: FileFingerprint,
    pre_run_directory_chain: DirectoryChainFingerprint,
    post_run_directory_chain: DirectoryChainFingerprint,
) -> str:
    message = "\0".join(
        (
            agent_id,
            expected_model_ref,
            requested_session_key,
            reported_session_id,
            request_sha256,
            nonce,
            version,
            expected_sha256,
            actual_sha256,
            pre_run_actual_sha256,
            post_run_actual_sha256,
            ":".join(str(value) for value in pre_run_file_fingerprint),
            ":".join(str(value) for value in post_run_file_fingerprint),
            "|".join(
                ":".join(str(value) for value in fingerprint)
                for fingerprint in pre_run_directory_chain
            ),
            "|".join(
                ":".join(str(value) for value in fingerprint)
                for fingerprint in post_run_directory_chain
            ),
            "match",
            "stable_across_run=true",
        )
    ).encode("ascii")
    return hmac.new(_PROOF_ATTESTATION_KEY, message, hashlib.sha256).hexdigest()


def _read_expected_contract(path: Path) -> bytes:
    path_stat = path.lstat()
    if not stat.S_ISREG(path_stat.st_mode):
        raise OSError("expected runtime contract is not a regular file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    file_descriptor = os.open(path, flags)
    try:
        opened_stat = os.fstat(file_descriptor)
        if (
            not stat.S_ISREG(opened_stat.st_mode)
            or opened_stat.st_dev != path_stat.st_dev
            or opened_stat.st_ino != path_stat.st_ino
        ):
            raise OSError("expected runtime contract identity changed")
        with os.fdopen(file_descriptor, "rb") as expected_file:
            file_descriptor = -1
            return expected_file.read()
    finally:
        if file_descriptor >= 0:
            os.close(file_descriptor)


def _directory_open_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _open_state_directory(
    state_directory: Path,
) -> tuple[int, DirectoryChainFingerprint]:
    try:
        current_descriptor = os.open(state_directory.anchor, _directory_open_flags())
    except OSError as exc:
        raise _StateDirectoryIOError from exc
    try:
        try:
            root_stat = os.fstat(current_descriptor)
        except OSError as exc:
            raise _StateDirectoryIOError from exc
        directory_chain: list[FileFingerprint] = [_file_fingerprint(root_stat)]
        for component in state_directory.parts[1:]:
            try:
                component_stat = os.stat(
                    component,
                    dir_fd=current_descriptor,
                    follow_symlinks=False,
                )
            except (FileNotFoundError, NotADirectoryError) as exc:
                raise _UnsafeStateDirectoryError from exc
            except OSError as exc:
                raise _StateDirectoryIOError from exc
            if not stat.S_ISDIR(component_stat.st_mode):
                raise _UnsafeStateDirectoryError
            try:
                next_descriptor = os.open(
                    component,
                    _directory_open_flags(),
                    dir_fd=current_descriptor,
                )
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOENT, errno.ENOTDIR}:
                    raise _UnsafeStateDirectoryError from exc
                raise _StateDirectoryIOError from exc
            try:
                try:
                    opened_stat = os.fstat(next_descriptor)
                except OSError as exc:
                    raise _StateDirectoryIOError from exc
                if (
                    not stat.S_ISDIR(opened_stat.st_mode)
                    or opened_stat.st_dev != component_stat.st_dev
                    or opened_stat.st_ino != component_stat.st_ino
                ):
                    raise _UnsafeStateDirectoryError
            except Exception:
                os.close(next_descriptor)
                raise
            os.close(current_descriptor)
            current_descriptor = next_descriptor
            directory_chain.append(_file_fingerprint(opened_stat))
        return current_descriptor, tuple(directory_chain)
    except Exception:
        os.close(current_descriptor)
        raise


def _open_workspace(
    state_descriptor: int,
    workspace_name: str,
) -> tuple[int, FileFingerprint]:
    try:
        workspace_stat = os.stat(
            workspace_name,
            dir_fd=state_descriptor,
            follow_symlinks=False,
        )
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise _UnsafeWorkspaceError from exc
    except OSError as exc:
        raise _WorkspaceIOError from exc
    if not stat.S_ISDIR(workspace_stat.st_mode):
        raise _UnsafeWorkspaceError
    try:
        workspace_descriptor = os.open(
            workspace_name,
            _directory_open_flags(),
            dir_fd=state_descriptor,
        )
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOENT, errno.ENOTDIR}:
            raise _UnsafeWorkspaceError from exc
        raise _WorkspaceIOError from exc
    try:
        opened_stat = os.fstat(workspace_descriptor)
    except OSError as exc:
        os.close(workspace_descriptor)
        raise _WorkspaceIOError from exc
    if (
        not stat.S_ISDIR(opened_stat.st_mode)
        or opened_stat.st_dev != workspace_stat.st_dev
        or opened_stat.st_ino != workspace_stat.st_ino
    ):
        os.close(workspace_descriptor)
        raise _UnsafeWorkspaceError
    return workspace_descriptor, _file_fingerprint(opened_stat)


def _file_fingerprint(file_stat: os.stat_result) -> FileFingerprint:
    return (
        file_stat.st_dev,
        file_stat.st_ino,
        file_stat.st_size,
        file_stat.st_mtime_ns,
        file_stat.st_ctime_ns,
    )


def _fingerprint_payload(fingerprint: FileFingerprint) -> dict[str, int]:
    return dict(
        zip(
            ("device", "inode", "size", "mtime_ns", "ctime_ns"),
            fingerprint,
            strict=True,
        )
    )


def _directory_chain_payload(
    directory_chain: DirectoryChainFingerprint,
) -> list[dict[str, int]]:
    return [_fingerprint_payload(fingerprint) for fingerprint in directory_chain]


def _parsed_fingerprint(value: object) -> FileFingerprint | None:
    if not isinstance(value, dict):
        return None
    fields = ("device", "inode", "size", "mtime_ns", "ctime_ns")
    values = tuple(value.get(field) for field in fields)
    if not all(isinstance(item, int) and item >= 0 for item in values):
        return None
    return values  # type: ignore[return-value]


def _parsed_directory_chain(value: object) -> DirectoryChainFingerprint | None:
    if not isinstance(value, list) or not value:
        return None
    parsed = tuple(_parsed_fingerprint(item) for item in value)
    if any(item is None for item in parsed):
        return None
    return parsed  # type: ignore[return-value]


def _read_live_contract(
    workspace_descriptor: int,
) -> tuple[bytes | None, FileFingerprint | None]:
    try:
        target_stat = os.stat(
            "AGENTS.md",
            dir_fd=workspace_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return None, None
    if not stat.S_ISREG(target_stat.st_mode):
        raise _UnsafeTargetError
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    file_descriptor = os.open(
        "AGENTS.md",
        flags,
        dir_fd=workspace_descriptor,
    )
    try:
        opened_stat = os.fstat(file_descriptor)
        if (
            not stat.S_ISREG(opened_stat.st_mode)
            or opened_stat.st_dev != target_stat.st_dev
            or opened_stat.st_ino != target_stat.st_ino
        ):
            raise _UnsafeTargetError
        with os.fdopen(file_descriptor, "rb") as target_file:
            file_descriptor = -1
            content = target_file.read()
            final_stat = os.fstat(target_file.fileno())
    finally:
        if file_descriptor >= 0:
            os.close(file_descriptor)
    if (
        final_stat.st_size != opened_stat.st_size
        or final_stat.st_mtime_ns != opened_stat.st_mtime_ns
    ):
        raise _UnsafeTargetError
    return content, _file_fingerprint(final_stat)


def _state_directory() -> Path:
    try:
        if "OPENCLAW_STATE_DIR" not in os.environ:
            state_directory = Path.home() / ".openclaw"
        else:
            configured = os.environ["OPENCLAW_STATE_DIR"]
            if not configured or not configured.strip() or "\0" in configured:
                raise _UnsafeStateDirectoryError
            state_directory = Path(configured).expanduser()
    except (RuntimeError, ValueError) as exc:
        raise _UnsafeStateDirectoryError from exc
    if not state_directory.is_absolute() or ".." in state_directory.parts:
        raise _UnsafeStateDirectoryError
    return state_directory


def capture_runtime_contract(*, agent_id: str) -> RuntimeContractSnapshot:
    expected_path = _EXPECTED_CONTRACTS.get(agent_id)
    workspace_name = _WORKSPACE_NAMES.get(agent_id)
    if expected_path is None or workspace_name is None:
        return RuntimeContractSnapshot(
            version=RUNTIME_CONTRACT_VERSION,
            expected_sha256="unavailable",
            actual_sha256=None,
            status="unsupported_agent",
        )
    try:
        expected = _read_expected_contract(expected_path)
    except OSError:
        return RuntimeContractSnapshot(
            version=RUNTIME_CONTRACT_VERSION,
            expected_sha256="unavailable",
            actual_sha256=None,
            status="expected_unavailable",
        )
    expected_sha256 = _sha256(expected)
    try:
        state_descriptor, directory_chain = _open_state_directory(_state_directory())
    except _UnsafeStateDirectoryError:
        return RuntimeContractSnapshot(
            version=RUNTIME_CONTRACT_VERSION,
            expected_sha256=expected_sha256,
            actual_sha256=None,
            status="unsafe_state_dir",
        )
    except _StateDirectoryIOError:
        return RuntimeContractSnapshot(
            version=RUNTIME_CONTRACT_VERSION,
            expected_sha256=expected_sha256,
            actual_sha256=None,
            status="io_error",
        )
    try:
        try:
            workspace_descriptor, workspace_fingerprint = _open_workspace(
                state_descriptor,
                workspace_name,
            )
        except _UnsafeWorkspaceError:
            return RuntimeContractSnapshot(
                version=RUNTIME_CONTRACT_VERSION,
                expected_sha256=expected_sha256,
                actual_sha256=None,
                status="unsafe_workspace",
            )
        except _WorkspaceIOError:
            return RuntimeContractSnapshot(
                version=RUNTIME_CONTRACT_VERSION,
                expected_sha256=expected_sha256,
                actual_sha256=None,
                status="io_error",
            )
    finally:
        os.close(state_descriptor)
    try:
        try:
            actual, file_fingerprint = _read_live_contract(workspace_descriptor)
        except _UnsafeTargetError:
            return RuntimeContractSnapshot(
                version=RUNTIME_CONTRACT_VERSION,
                expected_sha256=expected_sha256,
                actual_sha256=None,
                status="unsafe_target",
            )
        except OSError:
            return RuntimeContractSnapshot(
                version=RUNTIME_CONTRACT_VERSION,
                expected_sha256=expected_sha256,
                actual_sha256=None,
                status="io_error",
            )
    finally:
        os.close(workspace_descriptor)
    if actual is None:
        return RuntimeContractSnapshot(
            version=RUNTIME_CONTRACT_VERSION,
            expected_sha256=expected_sha256,
            actual_sha256=None,
            status="missing",
        )
    actual_sha256 = _sha256(actual)
    return RuntimeContractSnapshot(
        version=RUNTIME_CONTRACT_VERSION,
        expected_sha256=expected_sha256,
        actual_sha256=actual_sha256,
        status="match" if actual_sha256 == expected_sha256 else "mismatch",
        file_fingerprint=file_fingerprint,
        directory_chain_fingerprint=(*directory_chain, workspace_fingerprint),
    )


def require_runtime_contract_match(
    snapshot: RuntimeContractSnapshot,
    *,
    stage: str,
) -> RuntimeContractSnapshot:
    if snapshot.matches:
        return snapshot
    raise RuntimeContractVerificationError(
        "OpenClaw runtime contract verification failed: "
        f"stage={stage} status={snapshot.status} version={snapshot.version} "
        f"expected_sha256={snapshot.expected_sha256} "
        f"actual_sha256={snapshot.actual_sha256 or 'unavailable'}"
    )


def stable_runtime_contract_proof(
    *,
    agent_id: str,
    expected_model_ref: str,
    requested_session_key: str,
    reported_session_id: str,
    request_sha256: str,
    before: RuntimeContractSnapshot,
    after: RuntimeContractSnapshot,
) -> dict[str, object]:
    require_runtime_contract_match(before, stage="pre_run")
    if (
        not after.matches
        or after.version != before.version
        or after.expected_sha256 != before.expected_sha256
        or after.actual_sha256 != before.actual_sha256
        or before.file_fingerprint is None
        or after.file_fingerprint != before.file_fingerprint
        or before.directory_chain_fingerprint is None
        or after.directory_chain_fingerprint != before.directory_chain_fingerprint
    ):
        raise RuntimeContractVerificationError(
            "OpenClaw runtime contract verification failed: "
            f"stage=changed_during_run status={after.status} "
            f"version={after.version} expected_sha256={after.expected_sha256} "
            f"actual_sha256={after.actual_sha256 or 'unavailable'}"
        )
    assert before.actual_sha256 is not None
    assert after.actual_sha256 is not None
    assert before.file_fingerprint is not None
    assert after.file_fingerprint is not None
    assert before.directory_chain_fingerprint is not None
    assert after.directory_chain_fingerprint is not None
    nonce = secrets.token_hex(16)
    proof: dict[str, object] = _IssuedRuntimeContractProof({
        "agent_id": agent_id,
        "expected_model_ref": expected_model_ref,
        "requested_session_key": requested_session_key,
        "reported_session_id": reported_session_id,
        "request_sha256": request_sha256,
        "nonce": nonce,
        "version": before.version,
        "expected_sha256": before.expected_sha256,
        "actual_sha256": after.actual_sha256,
        "pre_run_actual_sha256": before.actual_sha256,
        "post_run_actual_sha256": after.actual_sha256,
        "pre_run_file_fingerprint": _fingerprint_payload(before.file_fingerprint),
        "post_run_file_fingerprint": _fingerprint_payload(after.file_fingerprint),
        "pre_run_directory_chain": _directory_chain_payload(
            before.directory_chain_fingerprint
        ),
        "post_run_directory_chain": _directory_chain_payload(
            after.directory_chain_fingerprint
        ),
        "status": "match",
        "stable_across_run": True,
    })
    attestation = _proof_attestation(
        agent_id=agent_id,
        expected_model_ref=expected_model_ref,
        requested_session_key=requested_session_key,
        reported_session_id=reported_session_id,
        request_sha256=request_sha256,
        nonce=nonce,
        version=before.version,
        expected_sha256=before.expected_sha256,
        actual_sha256=after.actual_sha256,
        pre_run_actual_sha256=before.actual_sha256,
        post_run_actual_sha256=after.actual_sha256,
        pre_run_file_fingerprint=before.file_fingerprint,
        post_run_file_fingerprint=after.file_fingerprint,
        pre_run_directory_chain=before.directory_chain_fingerprint,
        post_run_directory_chain=after.directory_chain_fingerprint,
    )
    proof["attestation"] = attestation
    _register_issued_proof(nonce=nonce, attestation=attestation)
    return proof


def runtime_contract_proof_is_valid(
    value: object,
    *,
    agent_id: str,
    expected_model_ref: str,
    requested_session_key: str,
    reported_session_id: str,
    request_sha256: str,
    consume: bool = False,
) -> bool:
    if type(value) is not _IssuedRuntimeContractProof:
        return False
    assert isinstance(value, dict)
    hashes = (
        value.get("expected_sha256"),
        value.get("actual_sha256"),
        value.get("pre_run_actual_sha256"),
        value.get("post_run_actual_sha256"),
    )
    if not (
        value.get("version") == RUNTIME_CONTRACT_VERSION
        and value.get("agent_id") == agent_id
        and value.get("expected_model_ref") == expected_model_ref
        and value.get("requested_session_key") == requested_session_key
        and value.get("reported_session_id") == reported_session_id
        and value.get("request_sha256") == request_sha256
        and _SHA256_PATTERN.fullmatch(request_sha256) is not None
        and bool(requested_session_key)
        and bool(reported_session_id)
        and isinstance(value.get("nonce"), str)
        and _NONCE_PATTERN.fullmatch(str(value["nonce"])) is not None
        and value.get("status") == "match"
        and value.get("stable_across_run") is True
        and all(
            isinstance(item, str) and _SHA256_PATTERN.fullmatch(item) is not None
            for item in hashes
        )
        and len(set(hashes)) == 1
    ):
        return False
    expected_sha256, actual_sha256, pre_run_sha256, post_run_sha256 = hashes
    assert isinstance(expected_sha256, str)
    assert isinstance(actual_sha256, str)
    assert isinstance(pre_run_sha256, str)
    assert isinstance(post_run_sha256, str)
    pre_run_fingerprint = _parsed_fingerprint(value.get("pre_run_file_fingerprint"))
    post_run_fingerprint = _parsed_fingerprint(value.get("post_run_file_fingerprint"))
    if (
        pre_run_fingerprint is None
        or post_run_fingerprint is None
        or pre_run_fingerprint != post_run_fingerprint
    ):
        return False
    pre_run_directory_chain = _parsed_directory_chain(
        value.get("pre_run_directory_chain")
    )
    post_run_directory_chain = _parsed_directory_chain(
        value.get("post_run_directory_chain")
    )
    if (
        pre_run_directory_chain is None
        or post_run_directory_chain is None
        or pre_run_directory_chain != post_run_directory_chain
    ):
        return False
    attestation = value.get("attestation")
    if not (
        isinstance(attestation, str)
        and hmac.compare_digest(
            attestation,
            _proof_attestation(
                agent_id=agent_id,
                expected_model_ref=expected_model_ref,
                requested_session_key=requested_session_key,
                reported_session_id=reported_session_id,
                request_sha256=request_sha256,
                nonce=str(value["nonce"]),
                version=RUNTIME_CONTRACT_VERSION,
                expected_sha256=expected_sha256,
                actual_sha256=actual_sha256,
                pre_run_actual_sha256=pre_run_sha256,
                post_run_actual_sha256=post_run_sha256,
                pre_run_file_fingerprint=pre_run_fingerprint,
                post_run_file_fingerprint=post_run_fingerprint,
                pre_run_directory_chain=pre_run_directory_chain,
                post_run_directory_chain=post_run_directory_chain,
            ),
        )
    ):
        return False
    return _registered_proof_is_valid(
        nonce=str(value["nonce"]),
        attestation=attestation,
        consume=consume,
    )
