from __future__ import annotations

from contextlib import nullcontext
import importlib.util
from pathlib import Path
from typing import Any
from unittest.mock import ANY

import pytest


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "refresh_execution_position_truth.py"
)
SCRIPT_SPEC = importlib.util.spec_from_file_location(
    "refresh_execution_position_truth_cli",
    SCRIPT_PATH,
)
assert SCRIPT_SPEC is not None and SCRIPT_SPEC.loader is not None
cli = importlib.util.module_from_spec(SCRIPT_SPEC)
SCRIPT_SPEC.loader.exec_module(cli)


class _SettingsStub:
    def __init__(self) -> None:
        self.database_url = ""
        self.runtime_mode = ""

    def model_copy(self, *, update: dict[str, str]) -> _SettingsStub:
        self.database_url = update["database_url"]
        self.runtime_mode = update["runtime_mode"]
        return self


class _EngineStub:
    disposed = False

    def dispose(self) -> None:
        self.disposed = True


def _stub_runtime(monkeypatch: pytest.MonkeyPatch) -> tuple[object, _EngineStub]:
    db = object()
    engine = _EngineStub()
    monkeypatch.setattr(cli, "load_settings", _SettingsStub)
    monkeypatch.setattr(cli, "create_db_engine", lambda settings: engine)
    monkeypatch.setattr(
        cli,
        "create_session_factory",
        lambda engine: lambda: nullcontext(db),
    )
    return db, engine


def test_legacy_attribution_exchange_routes_dry_run_without_mutation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db, engine = _stub_runtime(monkeypatch)
    adapter = object()
    calls: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(cli, "KisBrokerAdapter", lambda *, settings: adapter)
    monkeypatch.setattr(
        cli,
        "analyze_position_truth_remediation",
        lambda **kwargs: pytest.fail("legacy dry-run used stale-truth analysis"),
    )
    monkeypatch.setattr(
        cli,
        "apply_position_truth_remediation",
        lambda **kwargs: pytest.fail("dry-run attempted stale-truth mutation"),
    )
    monkeypatch.setattr(
        cli,
        "analyze_legacy_position_attribution",
        lambda **kwargs: calls.append(("analyze", kwargs)) or {"mode": "dry-run"},
        raising=False,
    )
    monkeypatch.setattr(
        cli,
        "apply_legacy_position_attribution",
        lambda **kwargs: pytest.fail("dry-run attempted legacy attribution mutation"),
        raising=False,
    )

    assert (
        cli.main(
            [
                "--database-url",
                "sqlite:///test.db",
                "--runtime-mode",
                "paper",
                "--legacy-attribution-exchange",
                "KRX",
            ]
        )
        == 0
    )

    assert calls == [
        (
            "analyze",
            {
                "settings": ANY,
                "db": db,
                "auth_session_factory": ANY,
                "adapter": adapter,
                "exchange": "KRX",
            },
        )
    ]
    assert '"mode": "dry-run"' in capsys.readouterr().out
    assert engine.disposed is True


def test_legacy_attribution_apply_routes_explicit_approvals_and_audit_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db, engine = _stub_runtime(monkeypatch)
    adapter = object()
    calls: list[dict[str, Any]] = []
    first_fingerprint = "a" * 64
    second_fingerprint = "B" * 64
    monkeypatch.setattr(cli, "KisBrokerAdapter", lambda *, settings: adapter)
    monkeypatch.setattr(
        cli,
        "apply_position_truth_remediation",
        lambda **kwargs: pytest.fail("legacy apply used stale-truth remediation"),
    )
    monkeypatch.setattr(
        cli,
        "analyze_legacy_position_attribution",
        lambda **kwargs: pytest.fail("apply performed only a dry-run"),
    )
    monkeypatch.setattr(
        cli,
        "apply_legacy_position_attribution",
        lambda **kwargs: calls.append(kwargs) or {"mode": "apply"},
    )

    assert (
        cli.main(
            [
                "--database-url",
                "sqlite:///test.db",
                "--runtime-mode",
                "paper",
                "--legacy-attribution-exchange",
                "NASD",
                "--apply",
                "--approve-position",
                f"position-1={first_fingerprint}",
                "--approve-position",
                f"position-2={second_fingerprint}",
                "--actor",
                "  operator  ",
                "--reason",
                "  broker proof verified  ",
            ]
        )
        == 0
    )

    assert calls == [
        {
            "settings": ANY,
            "db": db,
            "auth_session_factory": ANY,
            "adapter": adapter,
            "exchange": "NASD",
            "approved_evidence": {
                "position-1": first_fingerprint,
                "position-2": second_fingerprint.lower(),
            },
            "actor": "operator",
            "reason": "broker proof verified",
        }
    ]
    assert engine.disposed is True


def test_live_remediation_accepts_database_url_from_environment_without_argv_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db, engine = _stub_runtime(monkeypatch)
    calls: list[dict[str, Any]] = []
    database_url = "postgresql+psycopg://operator:secret@db.example/exec"
    monkeypatch.setenv("KAI_EXEC_DATABASE_URL", database_url)
    monkeypatch.setattr(
        cli,
        "analyze_position_truth_remediation",
        lambda **kwargs: calls.append(kwargs) or {"mode": "dry-run"},
    )

    assert cli.main(["--runtime-mode", "live"]) == 0

    assert len(calls) == 1
    assert calls[0]["db"] is db
    assert calls[0]["settings"].database_url == database_url
    assert calls[0]["settings"].runtime_mode == "live"
    assert engine.disposed is True


def test_database_url_with_password_is_rejected_from_argv_before_db_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("KAI_EXEC_DATABASE_URL", raising=False)
    monkeypatch.setattr(
        cli,
        "create_db_engine",
        lambda settings: pytest.fail("credential-bearing argv opened the database"),
    )

    with pytest.raises(SystemExit, match="KAI_EXEC_DATABASE_URL environment variable"):
        cli.main(
            [
                "--database-url",
                "postgresql+psycopg://operator:secret@db.example/exec",
                "--runtime-mode",
                "live",
            ]
        )


def test_query_embedded_database_secret_is_rejected_before_settings_or_db_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = (
        "postgresql://db.example/exec?connection_string="
        "host%3Ddb.example%3Bpassword%3Dmust-not-leak-position-secret"
    )
    monkeypatch.delenv("KAI_EXEC_DATABASE_URL", raising=False)
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: pytest.fail("credential-bearing argv loaded settings"),
    )
    monkeypatch.setattr(
        cli,
        "create_db_engine",
        lambda settings: pytest.fail("credential-bearing argv opened the database"),
    )

    with pytest.raises(SystemExit) as caught:
        cli.main(
            [
                "--database-url",
                database_url,
                "--runtime-mode",
                "live",
            ]
        )

    message = str(caught.value)
    assert "KAI_EXEC_DATABASE_URL environment variable" in message
    assert "must-not-leak-position-secret" not in message
    assert database_url not in message


@pytest.mark.parametrize(
    "approvals",
    [
        [f"={'a' * 64}"],
        ["position-1="],
        [f"position-1={'a' * 63}"],
        [f"position-1={'a' * 65}"],
        [f"position-1={'g' * 64}"],
        [f"position-1={'a' * 64}=extra"],
        [f"position-1={'a' * 64}", f" position-1 ={'b' * 64}"],
    ],
)
def test_legacy_attribution_rejects_invalid_or_duplicate_approvals_before_db_open(
    monkeypatch: pytest.MonkeyPatch,
    approvals: list[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "create_db_engine",
        lambda settings: pytest.fail("invalid approval opened the database"),
    )
    argv = [
        "--database-url",
        "sqlite:///must-not-open.db",
        "--runtime-mode",
        "paper",
        "--legacy-attribution-exchange",
        "KRX",
        "--apply",
        "--actor",
        "operator",
        "--reason",
        "broker proof verified",
    ]
    for approval in approvals:
        argv.extend(("--approve-position", approval))

    with pytest.raises(SystemExit, match="approved position evidence is invalid"):
        cli.main(argv)


@pytest.mark.parametrize(
    ("extra_args", "expected_error"),
    [
        (
            ["--legacy-attribution-exchange", "KRX", "--apply"],
            "legacy attribution apply requires at least one --approve-position",
        ),
        (
            [
                "--legacy-attribution-exchange",
                "KRX",
                "--approve-position",
                f"position-1={'a' * 64}",
            ],
            "--approve-position is valid only with legacy attribution --apply",
        ),
        (
            [
                "--apply",
                "--approve-position",
                f"position-1={'a' * 64}",
            ],
            "--approve-position is valid only with legacy attribution --apply",
        ),
    ],
)
def test_legacy_attribution_rejects_unsafe_mode_combinations_before_db_open(
    monkeypatch: pytest.MonkeyPatch,
    extra_args: list[str],
    expected_error: str,
) -> None:
    monkeypatch.setattr(
        cli,
        "create_db_engine",
        lambda settings: pytest.fail("unsafe CLI combination opened the database"),
    )

    with pytest.raises(SystemExit, match=expected_error):
        cli.main(
            [
                "--database-url",
                "sqlite:///must-not-open.db",
                "--runtime-mode",
                "paper",
                "--actor",
                "operator",
                "--reason",
                "broker proof verified",
                *extra_args,
            ]
        )


@pytest.mark.parametrize("exchange", ["krx", "OTCB", "ALL"])
def test_legacy_attribution_requires_an_exact_supported_exchange_before_db_open(
    monkeypatch: pytest.MonkeyPatch,
    exchange: str,
) -> None:
    monkeypatch.setattr(
        cli,
        "create_db_engine",
        lambda settings: pytest.fail("unsupported exchange opened the database"),
    )

    with pytest.raises(SystemExit):
        cli.main(
            [
                "--database-url",
                "sqlite:///must-not-open.db",
                "--runtime-mode",
                "paper",
                "--legacy-attribution-exchange",
                exchange,
            ]
        )


def test_default_dry_run_preserves_stale_truth_analysis_without_broker_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db, engine = _stub_runtime(monkeypatch)
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(
        cli,
        "KisBrokerAdapter",
        lambda *, settings: pytest.fail("stale dry-run created a broker adapter"),
    )
    monkeypatch.setattr(
        cli,
        "analyze_position_truth_remediation",
        lambda **kwargs: calls.append(kwargs) or {"mode": "dry-run"},
    )
    monkeypatch.setattr(
        cli,
        "apply_position_truth_remediation",
        lambda **kwargs: pytest.fail("stale dry-run attempted mutation"),
    )

    assert (
        cli.main(
            [
                "--database-url",
                "sqlite:///test.db",
                "--runtime-mode",
                "paper",
            ]
        )
        == 0
    )

    assert calls == [{"settings": ANY, "db": db}]
    assert engine.disposed is True


def test_default_apply_preserves_stale_truth_routing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db, engine = _stub_runtime(monkeypatch)
    adapter = object()
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(cli, "KisBrokerAdapter", lambda *, settings: adapter)
    monkeypatch.setattr(
        cli,
        "apply_position_truth_remediation",
        lambda **kwargs: calls.append(kwargs) or {"mode": "apply"},
    )
    monkeypatch.setattr(
        cli,
        "apply_legacy_position_attribution",
        lambda **kwargs: pytest.fail("stale apply used legacy attribution"),
    )

    assert (
        cli.main(
            [
                "--database-url",
                "sqlite:///test.db",
                "--runtime-mode",
                "paper",
                "--apply",
                "--actor",
                "  operator  ",
                "--reason",
                "  refresh stale truth  ",
            ]
        )
        == 0
    )

    assert calls == [
        {
            "settings": ANY,
            "db": db,
            "auth_session_factory": ANY,
            "adapter": adapter,
            "actor": "operator",
            "reason": "refresh stale truth",
        }
    ]
    assert engine.disposed is True


def test_live_legacy_attribution_accepts_environment_database_url_without_argv_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db, engine = _stub_runtime(monkeypatch)
    adapter = object()
    calls: list[dict[str, Any]] = []
    database_url = f"sqlite:///{tmp_path / 'legacy-live.db'}"
    monkeypatch.setenv(
        "KAI_EXEC_DATABASE_URL",
        database_url,
    )
    monkeypatch.setattr(
        cli, "KisBrokerAdapter", lambda *, settings: adapter
    )
    monkeypatch.setattr(
        cli,
        "analyze_legacy_position_attribution",
        lambda **kwargs: calls.append(kwargs) or {"mode": "dry-run"},
    )

    assert cli.main(
        [
            "--runtime-mode",
            "live",
            "--legacy-attribution-exchange",
            "KRX",
        ]
    ) == 0

    assert len(calls) == 1
    assert calls[0]["settings"].database_url == database_url
    assert calls[0]["db"] is db
    assert calls[0]["adapter"] is adapter
    assert engine.disposed is True
