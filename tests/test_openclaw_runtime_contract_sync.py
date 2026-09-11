from __future__ import annotations

import hashlib
import importlib.util
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "sync_openclaw_runtime_contract.py"
SCRIPT_SPEC = importlib.util.spec_from_file_location(
    "sync_openclaw_runtime_contract",
    SCRIPT,
)
assert SCRIPT_SPEC is not None
assert SCRIPT_SPEC.loader is not None
contract_sync = importlib.util.module_from_spec(SCRIPT_SPEC)
SCRIPT_SPEC.loader.exec_module(contract_sync)
TEMPLATE = (
    REPO_ROOT
    / "runtime_contracts"
    / "openclaw"
    / "v1"
    / "exec-agent"
    / "AGENTS.md"
)


def run_cli(openclaw_home: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            *arguments,
            "--openclaw-home",
            str(openclaw_home),
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def run_cli_with_environment(
    *arguments: str,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *arguments],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )


@pytest.mark.parametrize("operation", ["--check", "--install"])
def test_openclaw_state_dir_environment_is_used_when_home_flag_is_absent(
    tmp_path: Path,
    operation: str,
) -> None:
    configured_state = tmp_path / "configured-state"
    target = configured_state / "workspace-exec-agent" / "AGENTS.md"
    target.parent.mkdir(parents=True)
    target.write_text("stale contract\n", encoding="utf-8")
    if operation == "--check":
        target.write_bytes(TEMPLATE.read_bytes())
    environment = {
        **os.environ,
        "HOME": str(tmp_path / "unused-home"),
        "OPENCLAW_STATE_DIR": str(configured_state),
    }

    result = run_cli_with_environment(
        operation,
        "--agent",
        "exec-agent",
        environment=environment,
    )

    assert result.returncode == 0, result.stderr
    assert "status=match" in result.stdout or "status=installed" in result.stdout
    assert target.read_bytes() == TEMPLATE.read_bytes()


def test_explicit_openclaw_home_overrides_state_dir_environment(
    tmp_path: Path,
) -> None:
    explicit_state = tmp_path / "explicit-state"
    explicit_target = explicit_state / "workspace-exec-agent" / "AGENTS.md"
    explicit_target.parent.mkdir(parents=True)
    explicit_target.write_bytes(TEMPLATE.read_bytes())
    environment_state = tmp_path / "environment-state"
    environment_target = environment_state / "workspace-exec-agent" / "AGENTS.md"
    environment_target.parent.mkdir(parents=True)
    environment_target.write_text("environment drift\n", encoding="utf-8")
    environment = {
        **os.environ,
        "OPENCLAW_STATE_DIR": str(environment_state),
    }

    result = run_cli_with_environment(
        "--check",
        "--agent",
        "exec-agent",
        "--openclaw-home",
        str(explicit_state),
        environment=environment,
    )

    assert result.returncode == 0, result.stderr
    assert "status=match" in result.stdout


def test_default_check_is_read_only_and_reports_version_and_hash(tmp_path: Path) -> None:
    openclaw_home = tmp_path / ".openclaw"
    target = openclaw_home / "workspace-exec-agent" / "AGENTS.md"
    target.parent.mkdir(parents=True)
    content = TEMPLATE.read_bytes()
    target.write_bytes(content)
    before = target.stat()

    result = run_cli(openclaw_home, "--agent", "exec-agent")

    assert result.returncode == 0, result.stderr
    assert "agent=exec-agent" in result.stdout
    assert "version=v1" in result.stdout
    assert f"expected_sha256={hashlib.sha256(content).hexdigest()}" in result.stdout
    assert "status=match" in result.stdout
    assert target.read_bytes() == content
    assert target.stat().st_mtime_ns == before.st_mtime_ns


@pytest.mark.parametrize("target_exists", [True, False])
def test_check_reports_drift_without_exposing_target_content(
    tmp_path: Path,
    target_exists: bool,
) -> None:
    openclaw_home = tmp_path / ".openclaw"
    target = openclaw_home / "workspace-exec-agent" / "AGENTS.md"
    target.parent.mkdir(parents=True)
    sensitive_drift = b"do-not-echo-this-runtime-content\n"
    if target_exists:
        target.write_bytes(sensitive_drift)

    result = run_cli(openclaw_home, "--check", "--agent", "exec-agent")

    assert result.returncode == 1
    assert "version=v1" in result.stdout
    assert "expected_sha256=" in result.stdout
    expected_status = "mismatch" if target_exists else "missing"
    expected_actual = (
        hashlib.sha256(sensitive_drift).hexdigest() if target_exists else "missing"
    )
    assert f"actual_sha256={expected_actual}" in result.stdout
    assert f"status={expected_status}" in result.stdout
    assert sensitive_drift.decode().strip() not in result.stdout


def test_install_atomically_replaces_target_and_preserves_mode(tmp_path: Path) -> None:
    openclaw_home = tmp_path / ".openclaw"
    target = openclaw_home / "workspace-exec-agent" / "AGENTS.md"
    target.parent.mkdir(parents=True)
    target.write_text("stale contract\n", encoding="utf-8")
    target.chmod(0o640)
    original_inode = target.stat().st_ino

    result = run_cli(openclaw_home, "--install", "--agent", "exec-agent")

    assert result.returncode == 0, result.stderr
    assert target.read_bytes() == TEMPLATE.read_bytes()
    assert target.stat().st_ino != original_inode
    assert stat.S_IMODE(target.stat().st_mode) == 0o640
    assert os.listdir(target.parent) == ["AGENTS.md"]
    assert "status=installed" in result.stdout


def test_install_creates_only_agents_file_with_private_mode(tmp_path: Path) -> None:
    openclaw_home = tmp_path / ".openclaw"
    workspace = openclaw_home / "workspace-exec-agent"
    workspace.mkdir(parents=True)
    adjacent = workspace / "runtime-state.json"
    adjacent.write_text('{"preserve": true}\n', encoding="utf-8")
    target = workspace / "AGENTS.md"

    result = run_cli(openclaw_home, "--install", "--agent", "exec-agent")

    assert result.returncode == 0, result.stderr
    assert target.exists()
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert adjacent.read_text(encoding="utf-8") == '{"preserve": true}\n'
    assert sorted(path.name for path in workspace.iterdir()) == [
        "AGENTS.md",
        "runtime-state.json",
    ]


@pytest.mark.parametrize("operation", ["--check", "--install"])
def test_symlink_target_fails_closed_without_mutation(
    tmp_path: Path,
    operation: str,
) -> None:
    openclaw_home = tmp_path / ".openclaw"
    target = openclaw_home / "workspace-exec-agent" / "AGENTS.md"
    target.parent.mkdir(parents=True)
    outside = tmp_path / "outside.md"
    outside.write_text("outside contract\n", encoding="utf-8")
    target.symlink_to(outside)

    result = run_cli(openclaw_home, operation, "--agent", "exec-agent")

    assert result.returncode != 0
    assert "status=unsafe_target" in result.stdout
    assert target.is_symlink()
    assert outside.read_text(encoding="utf-8") == "outside contract\n"


@pytest.mark.parametrize("operation", ["--check", "--install"])
def test_symlink_workspace_fails_closed_without_mutation(
    tmp_path: Path,
    operation: str,
) -> None:
    openclaw_home = tmp_path / ".openclaw"
    openclaw_home.mkdir()
    outside_workspace = tmp_path / "outside-workspace"
    outside_workspace.mkdir()
    outside_target = outside_workspace / "AGENTS.md"
    outside_target.write_text("outside contract\n", encoding="utf-8")
    (openclaw_home / "workspace-exec-agent").symlink_to(
        outside_workspace,
        target_is_directory=True,
    )

    result = run_cli(openclaw_home, operation, "--agent", "exec-agent")

    assert result.returncode != 0
    assert "status=unsafe_workspace" in result.stdout
    assert outside_target.read_text(encoding="utf-8") == "outside contract\n"


@pytest.mark.parametrize("operation", ["--check", "--install"])
def test_symlinked_state_directory_fails_closed_without_mutation(
    tmp_path: Path,
    operation: str,
) -> None:
    real_state = tmp_path / "real-state"
    outside_target = real_state / "workspace-exec-agent" / "AGENTS.md"
    outside_target.parent.mkdir(parents=True)
    outside_target.write_text("outside contract\n", encoding="utf-8")
    linked_state = tmp_path / "linked-state"
    linked_state.symlink_to(real_state, target_is_directory=True)

    result = run_cli(linked_state, operation, "--agent", "exec-agent")

    assert result.returncode == 2
    assert "status=unsafe_state_dir" in result.stdout
    assert outside_target.read_text(encoding="utf-8") == "outside contract\n"


@pytest.mark.parametrize("operation", ["--check", "--install"])
def test_state_directory_with_symlink_component_fails_closed_without_mutation(
    tmp_path: Path,
    operation: str,
) -> None:
    real_parent = tmp_path / "real-parent"
    real_state = real_parent / "state"
    outside_target = real_state / "workspace-exec-agent" / "AGENTS.md"
    outside_target.parent.mkdir(parents=True)
    outside_target.write_text("outside contract\n", encoding="utf-8")
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    result = run_cli(
        linked_parent / "state",
        operation,
        "--agent",
        "exec-agent",
    )

    assert result.returncode == 2
    assert "status=unsafe_state_dir" in result.stdout
    assert outside_target.read_text(encoding="utf-8") == "outside contract\n"


@pytest.mark.parametrize("scope", ["state", "workspace"])
def test_cli_directory_fstat_error_closes_descriptor(
    scope: str,
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_directory = tmp_path / ".openclaw"
    workspace = state_directory / "workspace-exec-agent"
    workspace.mkdir(parents=True)
    real_open = os.open
    real_fstat = os.fstat
    failed_descriptor: list[int] = []
    state_descriptor: int | None = None

    if scope == "workspace":
        state_descriptor, _state_chain = contract_sync._open_state_directory(
            state_directory
        )

    def tracking_open(path, flags, *args, **kwargs):
        descriptor = real_open(path, flags, *args, **kwargs)
        if scope == "state" and path == state_directory.anchor and not failed_descriptor:
            failed_descriptor.append(descriptor)
        elif scope == "workspace" and path == "workspace-exec-agent":
            failed_descriptor.append(descriptor)
        return descriptor

    def fail_selected_fstat(descriptor: int):
        if failed_descriptor and descriptor == failed_descriptor[-1]:
            raise PermissionError("simulated directory fstat failure")
        return real_fstat(descriptor)

    monkeypatch.setattr(contract_sync.os, "open", tracking_open)
    monkeypatch.setattr(contract_sync.os, "fstat", fail_selected_fstat)
    expected_error = (
        contract_sync.StateDirectoryIOError
        if scope == "state"
        else contract_sync.WorkspaceIOError
    )

    try:
        with pytest.raises(expected_error):
            if scope == "state":
                contract_sync._open_state_directory(state_directory)
            else:
                assert state_descriptor is not None
                contract_sync._open_workspace(
                    state_descriptor,
                    "workspace-exec-agent",
                )
    finally:
        if state_descriptor is not None:
            os.close(state_descriptor)

    assert failed_descriptor
    with pytest.raises(OSError):
        real_fstat(failed_descriptor[-1])


@pytest.mark.parametrize("operation", ["--check", "--install"])
@pytest.mark.parametrize("drift", ["replacement", "swap-restore"])
def test_cli_revalidates_state_path_after_target_read(
    operation: str,
    drift: str,
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    state_directory = tmp_path / ".openclaw"
    original_target = state_directory / "workspace-exec-agent" / "AGENTS.md"
    original_target.parent.mkdir(parents=True)
    original_content = (
        TEMPLATE.read_bytes() if operation == "--check" else b"stale contract\n"
    )
    original_target.write_bytes(original_content)
    replacement_state = tmp_path / "replacement-state"
    replacement_target = replacement_state / "workspace-exec-agent" / "AGENTS.md"
    replacement_target.parent.mkdir(parents=True)
    replacement_content = b"replacement contract\n"
    replacement_target.write_bytes(replacement_content)
    detached_state = tmp_path / "detached-state"
    real_read_target = contract_sync._read_target

    def read_then_drift(workspace_descriptor: int):
        result = real_read_target(workspace_descriptor)
        state_directory.rename(detached_state)
        replacement_state.rename(state_directory)
        if drift == "swap-restore":
            state_directory.rename(replacement_state)
            detached_state.rename(state_directory)
        return result

    monkeypatch.setattr(contract_sync, "_read_target", read_then_drift)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT),
            operation,
            "--agent",
            "exec-agent",
            "--openclaw-home",
            str(state_directory),
        ],
    )

    result = contract_sync.main()

    output = capsys.readouterr().out
    assert result == 2
    assert "status=unsafe_state_dir" in output
    assert "status=match" not in output
    assert "status=installed" not in output
    original_location = detached_state if drift == "replacement" else state_directory
    replacement_location = state_directory if drift == "replacement" else replacement_state
    assert (
        original_location / "workspace-exec-agent" / "AGENTS.md"
    ).read_bytes() == original_content
    assert (
        replacement_location / "workspace-exec-agent" / "AGENTS.md"
    ).read_bytes() == replacement_content


@pytest.mark.parametrize("drift", ["replacement", "swap-restore"])
def test_cli_revalidates_state_path_before_reporting_install_success(
    drift: str,
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    state_directory = tmp_path / ".openclaw"
    original_target = state_directory / "workspace-exec-agent" / "AGENTS.md"
    original_target.parent.mkdir(parents=True)
    original_target.write_text("stale contract\n", encoding="utf-8")
    replacement_state = tmp_path / "replacement-state"
    replacement_target = replacement_state / "workspace-exec-agent" / "AGENTS.md"
    replacement_target.parent.mkdir(parents=True)
    replacement_content = b"replacement contract\n"
    replacement_target.write_bytes(replacement_content)
    detached_state = tmp_path / "detached-state"
    real_atomic_install = contract_sync._atomic_install

    def install_then_drift(**kwargs):
        real_atomic_install(**kwargs)
        state_directory.rename(detached_state)
        replacement_state.rename(state_directory)
        if drift == "swap-restore":
            state_directory.rename(replacement_state)
            detached_state.rename(state_directory)

    monkeypatch.setattr(contract_sync, "_atomic_install", install_then_drift)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT),
            "--install",
            "--agent",
            "exec-agent",
            "--openclaw-home",
            str(state_directory),
        ],
    )

    result = contract_sync.main()

    output = capsys.readouterr().out
    assert result == 2
    assert "status=unsafe_state_dir" in output
    assert "status=installed" not in output
    original_location = detached_state if drift == "replacement" else state_directory
    replacement_location = state_directory if drift == "replacement" else replacement_state
    assert (
        original_location / "workspace-exec-agent" / "AGENTS.md"
    ).read_bytes() == TEMPLATE.read_bytes()
    assert (
        replacement_location / "workspace-exec-agent" / "AGENTS.md"
    ).read_bytes() == replacement_content


@pytest.mark.parametrize("operation", ["--check", "--install"])
def test_missing_workspace_fails_closed_without_creation(
    tmp_path: Path,
    operation: str,
) -> None:
    openclaw_home = tmp_path / ".openclaw"
    openclaw_home.mkdir()

    result = run_cli(openclaw_home, operation, "--agent", "exec-agent")

    assert result.returncode == 2
    assert "status=unsafe_workspace" in result.stdout
    assert list(openclaw_home.iterdir()) == []


@pytest.mark.parametrize("operation", ["--check", "--install"])
def test_non_regular_target_fails_closed(tmp_path: Path, operation: str) -> None:
    openclaw_home = tmp_path / ".openclaw"
    target = openclaw_home / "workspace-exec-agent" / "AGENTS.md"
    target.mkdir(parents=True)

    result = run_cli(openclaw_home, operation, "--agent", "exec-agent")

    assert result.returncode != 0
    assert "status=unsafe_target" in result.stdout
    assert target.is_dir()


@pytest.mark.parametrize("agent", ["signal-decider", "../exec-agent"])
def test_unknown_agent_and_path_traversal_are_rejected(
    tmp_path: Path,
    agent: str,
) -> None:
    openclaw_home = tmp_path / ".openclaw"
    openclaw_home.mkdir()

    result = run_cli(openclaw_home, "--install", "--agent", agent)

    assert result.returncode == 2
    assert list(openclaw_home.iterdir()) == []


def test_check_and_install_cannot_be_combined(tmp_path: Path) -> None:
    result = run_cli(
        tmp_path,
        "--check",
        "--install",
        "--agent",
        "exec-agent",
    )

    assert result.returncode == 2
