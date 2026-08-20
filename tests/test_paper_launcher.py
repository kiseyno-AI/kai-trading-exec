from pathlib import Path


def test_paper_launcher_enables_kis_us_automation_but_keeps_live_gate_closed() -> None:
    script = (Path(__file__).parents[1] / "start-alpaca-paper.sh").read_text()

    assert 'export KAI_EXEC_AUTONOMOUS_RUNTIME_ENABLED="true"' in script
    assert 'export KAI_EXEC_KIS_US_TRADING_ENABLED="true"' in script
    assert 'export KAI_EXEC_KIS_US_LIVE_TRADING_ENABLED="false"' in script
