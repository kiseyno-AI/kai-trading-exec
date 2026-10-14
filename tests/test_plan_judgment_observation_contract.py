from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from app.api.schemas.plan_judgment import PlanJudgmentActionRequest


@pytest.mark.parametrize(
    "payload",
    [
        {"risk_reasons": ["broad market risk"]},
        {"risk_flags": 3},
        {"risk_flags": 0, "risk_reasons": []},
        {
            "risk_flags": 0,
            "risk_reasons": [],
            "observation_id": "hourly-healthy-1",
            "market_date": "2026-10-13",
        },
    ],
)
def test_plan_judgment_request_requires_explicit_risk_and_healthy_identity(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        PlanJudgmentActionRequest.model_validate(payload)


def test_plan_judgment_request_keeps_legacy_risky_callers_fail_safe() -> None:
    request = PlanJudgmentActionRequest.model_validate(
        {"risk_flags": 3, "risk_reasons": [" broad market risk "]}
    )

    assert request.risk_flags == 3
    assert request.risk_reasons == ["broad market risk"]
    assert request.observation_id is None


def test_plan_judgment_request_accepts_explicit_healthy_observation() -> None:
    request = PlanJudgmentActionRequest.model_validate(
        {
            "risk_flags": 0,
            "risk_reasons": [],
            "observation_id": "hourly-healthy-1",
            "market_date": "2026-10-13",
            "source_observed_at": "2026-10-13T00:10:00Z",
        }
    )

    assert request.market_date == date(2026, 10, 13)
    assert request.source_observed_at == datetime(2026, 10, 13, 0, 10, tzinfo=UTC)


@pytest.mark.parametrize(
    "payload",
    [
        {"risk_flags": True, "risk_reasons": ["risk"]},
        {"risk_flags": "0", "risk_reasons": []},
        {"risk_flags": 0.5, "risk_reasons": []},
        {"risk_flags": None, "risk_reasons": []},
        {"risk_flags": 1, "risk_reasons": "risk"},
        {"risk_flags": 1, "risk_reasons": [1]},
        {"risk_flags": 1, "risk_reasons": None},
    ],
)
def test_plan_judgment_request_rejects_coerced_risk_material(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        PlanJudgmentActionRequest.model_validate(payload)
