import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[1]
LAUNCHERS = (
    "start-api.sh",
    "start-api-no-migrate.sh",
    "start-alpaca-paper.sh",
    "start-kis-us-paper.sh",
)


def test_launchers_default_to_supported_python_312() -> None:
    for launcher in LAUNCHERS:
        script = (ROOT / launcher).read_text()
        assert 'PYTHON_BIN="${PYTHON_BIN:-python3.12}"' in script


def test_launchers_require_nonblank_api_token_for_non_loopback_bind(tmp_path: Path) -> None:
    for launcher in LAUNCHERS:
        for token in (None, " \t "):
            source = ROOT / launcher
            copied = tmp_path / launcher
            copied.write_text(source.read_text())
            env = os.environ.copy()
            env.pop("HOST", None)
            if token is None:
                env.pop("KAI_EXEC_API_BEARER_TOKEN", None)
            else:
                env["KAI_EXEC_API_BEARER_TOKEN"] = token
            env["KAI_EXEC_API_HOST"] = "0.0.0.0"
            env["PYTHON_BIN"] = "/bin/true"

            result = subprocess.run(
                ["bash", str(copied)],
                cwd=tmp_path,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )

            assert result.returncode != 0, (launcher, token)
            assert "KAI_EXEC_API_BEARER_TOKEN is required" in result.stderr, (launcher, token)


def test_alpaca_paper_launcher_keeps_kis_autonomous_runtime_out_of_alpaca_db() -> None:
    script = (Path(__file__).parents[1] / "start-alpaca-paper.sh").read_text()

    assert "source ./.env.paper" in script
    assert script.index("source ./.env.paper") > script.index("source ./.env")
    assert 'export KAI_EXEC_DATABASE_URL="sqlite:///$ROOT_DIR/var/alpaca_paper_exec.db"' in script
    assert 'export KAI_EXEC_AUTONOMOUS_RUNTIME_ENABLED="false"' in script
    assert 'export KAI_EXEC_KIS_US_TRADING_ENABLED="false"' in script
    assert 'export KAI_EXEC_KIS_US_LIVE_TRADING_ENABLED="false"' in script


def test_kis_us_paper_launcher_uses_separate_db_and_keeps_live_gate_closed() -> None:
    script = (ROOT / "start-kis-us-paper.sh").read_text()

    assert "source ./.env.paper" in script
    assert script.index("source ./.env.paper") > script.index("source ./.env")
    assert 'PORT="${PORT:-8012}"' in script
    assert 'export KAI_EXEC_DATABASE_URL="sqlite:///$ROOT_DIR/var/kis_us_paper_exec.db"' in script
    assert 'export KAI_EXEC_AUTONOMOUS_RUNTIME_ENABLED="true"' in script
    assert 'export KAI_EXEC_KIS_US_TRADING_ENABLED="true"' in script
    assert 'export KAI_EXEC_KIS_US_LIVE_TRADING_ENABLED="false"' in script
    assert (
        'export KAI_EXEC_KIS_US_ALLOW_DEGRADED_REALIZED_PNL_COVERAGE="${KAI_EXEC_KIS_US_ALLOW_DEGRADED_REALIZED_PNL_COVERAGE:-true}"'
        in script
    )
    assert 'export KAI_EXEC_ALPACA_CRYPTO_MONITORING_ENABLED="false"' in script
    assert 'export KAI_EXEC_ALPACA_US_EQUITY_MONITORING_ENABLED="false"' in script
