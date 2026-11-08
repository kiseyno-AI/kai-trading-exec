import os
from pathlib import Path
import signal
import subprocess
import sys
from time import monotonic, sleep

import pytest

from app.services import trigger_time_exec_agent, watch_exec_agent_sizing


@pytest.mark.skipif(not hasattr(os, "fork") or not Path("/proc").is_dir(), reason="Linux process-tree test")
@pytest.mark.parametrize("runner", [trigger_time_exec_agent, watch_exec_agent_sizing])
def test_timed_out_agent_kills_child_after_group_leader_exits(
    runner,
    tmp_path,
    monkeypatch,
) -> None:
    child_pid_path = tmp_path / "child.pid"
    script = """
import os
from pathlib import Path
import signal
import sys

if os.fork() == 0:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    Path(sys.argv[1]).write_text(str(os.getpid()))
while True:
    signal.pause()
"""
    real_popen = subprocess.Popen
    processes = []

    def start_ready_process(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        processes.append(process)
        deadline = monotonic() + 5
        while not child_pid_path.exists():
            assert process.poll() is None, "test process exited before its child was ready"
            assert monotonic() < deadline, "test child did not become ready"
            sleep(0.005)
        return process

    def child_is_running(pid: int) -> bool:
        try:
            state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
        except FileNotFoundError:
            return False
        return state not in {"Z", "X"}

    monkeypatch.setattr(runner.subprocess, "Popen", start_ready_process)
    try:
        with pytest.raises(subprocess.TimeoutExpired):
            runner._run_exec_agent_command(
                [sys.executable, "-c", script, str(child_pid_path)],
                timeout_seconds=0.05,
            )
        assert processes[0].returncode == -signal.SIGTERM
        child_pid = int(child_pid_path.read_text())
        deadline = monotonic() + 1
        while child_is_running(child_pid) and monotonic() < deadline:
            sleep(0.005)
        assert not child_is_running(child_pid), "timed-out agent left a running child"
    finally:
        for process in processes:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=2)
            for pipe in (process.stdout, process.stderr):
                if pipe is not None:
                    pipe.close()
