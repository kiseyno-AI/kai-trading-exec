from pathlib import Path


ROOT = Path(__file__).parents[1]


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
    assert 'export KAI_EXEC_ALPACA_CRYPTO_MONITORING_ENABLED="false"' in script
    assert 'export KAI_EXEC_ALPACA_US_EQUITY_MONITORING_ENABLED="false"' in script
