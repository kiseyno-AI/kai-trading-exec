from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import select

from app.db.session import create_session_factory
from app.models.runtime import CoreBuyAuthorityState, ExecutionPlanItem
from app.services import core_buy_authority
from test_core_buy_authority import _command, _plan_item, _settings
from test_reconciliation_concurrency import reconciliation_database


@pytest.mark.parametrize("newer_block", (False, True))
def test_delayed_recovery_projection_preserves_newer_block(
    reconciliation_database, newer_block,
):
    database_settings, engine = reconciliation_database
    settings = _settings().model_copy(
        update={"database_url": database_settings.database_url}
    )
    session_factory = create_session_factory(engine)
    blocked_at = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    plan, item = _plan_item(status="watch_monitoring_active")
    with session_factory() as setup:
        setup.add(plan)
        setup.commit()
        item_id = item.id
        for offset, action in ((0, "block_new_buy"), (2, "allow")):
            observed_at = blocked_at + timedelta(seconds=offset)
            core_buy_authority.apply_core_buy_authority_observation(
                settings=settings, db=setup,
                command=_command(
                    action=action, source_observed_at=observed_at,
                    evaluated_at=observed_at,
                ),
                now=observed_at,
            )

    recovery_at = blocked_at + timedelta(seconds=4)
    recovery = _command(
        action="allow", source_observed_at=recovery_at, evaluated_at=recovery_at,
    )
    newer_at = recovery_at + timedelta(seconds=1)
    blocked = _command(
        action="block_new_buy", source_observed_at=newer_at, evaluated_at=newer_at,
    )
    original_project = core_buy_authority._project_scope

    def project_after_newer_block(**kwargs):
        if newer_block and kwargs["command"].observation_id == recovery.observation_id:
            with session_factory() as other:
                core_buy_authority.apply_core_buy_authority_observation(
                    settings=settings, db=other, command=blocked, now=newer_at,
                )
        return original_project(**kwargs)

    with session_factory() as first, patch.object(
        core_buy_authority, "_project_scope", side_effect=project_after_newer_block,
    ):
        acknowledgement = core_buy_authority.apply_core_buy_authority_observation(
            settings=settings, db=first, command=recovery, now=recovery_at,
        )

    with session_factory() as verify:
        state = verify.scalar(select(CoreBuyAuthorityState))
        current_item = verify.get(ExecutionPlanItem, item_id)
        assert state.fence_active is newer_block
        assert acknowledgement.fence_active is newer_block
        assert state.projection_pending is False
        assert state.last_observation_id == (
            blocked.observation_id if newer_block else recovery.observation_id
        )
        adjustment = current_item.risk_context["core_buy_authority"]
        assert adjustment["action"] == (
            "block_new_buy" if newer_block else "recovered"
        )
        if newer_block:
            assert adjustment["observation_id"] == blocked.observation_id
            assert state.healthy_observation_count == 0
