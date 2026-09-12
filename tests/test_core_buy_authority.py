from __future__ import annotations

import copy
from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from app.api.schemas.buy_authority import (
    CoreBuyAuthorityObservationV1,
    core_buy_authority_material_fingerprint,
)
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_db_engine, create_session_factory
from app.models.runtime import (
    CoreBuyAuthorityObservation,
    CoreBuyAuthorityState,
    ExecutionPlan,
    ExecutionPlanItem,
)
from app.services.core_buy_authority import (
    CoreBuyAuthorityConflictError,
    _project_scope,
    apply_core_buy_authority_observation,
    core_buy_authority_block_reason,
)
from app.services.execution_account_route import bind_kis_account_route
from app.services.monitoring import expire_prior_market_date_unfilled_watch_buys


def _settings(**updates: object) -> Settings:
    return Settings(
        database_url="sqlite:///:memory:",
        runtime_mode="live",
        runtime_broker_provider="kis",
        execution_position_account_scope="primary-kis-live",
        kis_account_number="12345678",
        kis_account_product_code="01",
        hourly_plan_judgment_recovery_required_healthy_observations=2,
        hourly_plan_judgment_recovery_cooldown_seconds=0,
        hourly_plan_judgment_recovery_min_observation_interval_seconds=1,
        **updates,
    )


def _payload(
    *,
    action: str,
    source_observed_at: datetime | None,
    evaluated_at: datetime,
    reason_codes: list[str] | None = None,
    market_date: date = date(2026, 9, 11),
) -> dict[str, object]:
    market_evidence = [
        {
            "market": market,
            "status": "available" if source_observed_at else "unavailable",
            "authoritative": source_observed_at is not None,
            "action": "allow" if action == "allow" else "block_new_buy",
            "observed_at": source_observed_at,
            "reason_code": "normal" if action == "allow" else "risk_off",
        }
        for market in ("KOSPI", "KOSDAQ")
    ]
    body: dict[str, object] = {
        "schema_version": "core_exec_buy_authority_observation_v1",
        "authority_schema_version": "current_live_market_buy_authority_v1",
        "source": "current_market_snapshot_cache",
        "scope": {
            "provider": "kis",
            "market": "KRX",
            "currency": "KRW",
            "runtime_mode": "live",
            "account_reference": "KIS:12345678-01",
        },
        "market_date": market_date,
        "evaluated_at": evaluated_at,
        "source_observed_at": source_observed_at,
        "action": action,
        "authoritative": source_observed_at is not None,
        "source_status": "available" if source_observed_at else "unavailable",
        "reason_codes": reason_codes or (["normal"] if action == "allow" else ["risk_off"]),
        "required_markets": ["KOSPI", "KOSDAQ"],
        "market_evidence": market_evidence,
    }
    fingerprint = core_buy_authority_material_fingerprint(body)
    body["material_fingerprint"] = fingerprint
    body["observation_id"] = f"buy-authority-v1:{fingerprint}"
    return body


def _command(**kwargs: object) -> CoreBuyAuthorityObservationV1:
    return CoreBuyAuthorityObservationV1.model_validate(_payload(**kwargs))


def _db(settings: Settings):
    engine = create_db_engine(settings)
    Base.metadata.create_all(engine)
    return engine, create_session_factory(engine)


def _plan_item(
    *,
    mode: str = "live",
    exchange: str = "KRX",
    currency: str = "KRW",
    core_v4: bool = True,
    status: str = "next_open_buy_scheduled",
) -> tuple[ExecutionPlan, ExecutionPlanItem]:
    route_settings = _settings()
    risk_context, quantity_policy = bind_kis_account_route(
        settings=route_settings,
        provider="kis",
        risk_context={
            "exchange": exchange,
            "currency": currency,
            "account_reference": "KIS:12345678-01",
            **(
                {
                    "_core_v4_contract_binding": {
                        "version": "core-v4-persisted-binding-v1",
                    }
                }
                if core_v4
                else {}
            ),
        },
        quantity_policy=None,
    )
    plan = ExecutionPlan(
        external_plan_id=f"plan-{mode}-{exchange}-{currency}-{core_v4}",
        source="kai-trading-core",
        market_date=date(2026, 9, 11),
        mode=mode,
        status="accepted",
    )
    item = ExecutionPlanItem(
        plan=plan,
        symbol="005930" if exchange == "KRX" else "AAPL",
        side="buy",
        intent="next_open_buy",
        status=status,
        provider="kis",
        venue="kis",
        asset_class="stock" if exchange == "KRX" else "us_equity",
        risk_context=risk_context,
        quantity_policy=quantity_policy,
    )
    return plan, item


def test_observation_contract_binds_canonical_material() -> None:
    observed_at = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    payload = _payload(
        action="allow",
        source_observed_at=observed_at,
        evaluated_at=observed_at + timedelta(seconds=1),
    )

    command = CoreBuyAuthorityObservationV1.model_validate(payload)

    assert command.material_fingerprint == core_buy_authority_material_fingerprint(payload)
    assert (
        command.material_fingerprint
        == "b26f85f5a98bcdd767a05e8a5b537994f58fd6705f53e09b66f07673841b94e8"
    )
    tampered = {**payload, "reason_codes": ["tampered"]}
    with pytest.raises(ValidationError, match="material fingerprint mismatch"):
        CoreBuyAuthorityObservationV1.model_validate(tampered)

    future_market = copy.deepcopy(payload)
    future_market["market_evidence"][1]["observed_at"] = (  # type: ignore[index]
        observed_at + timedelta(hours=1)
    )
    future_market["source_observed_at"] = observed_at
    future_fingerprint = core_buy_authority_material_fingerprint(future_market)
    future_market["material_fingerprint"] = future_fingerprint
    future_market["observation_id"] = f"buy-authority-v1:{future_fingerprint}"
    with pytest.raises(
        ValidationError,
        match="evaluated_at must not precede market evidence",
    ):
        CoreBuyAuthorityObservationV1.model_validate(future_market)

    offset_equivalent = copy.deepcopy(payload)
    seoul = ZoneInfo("Asia/Seoul")
    offset_equivalent["evaluated_at"] = (observed_at + timedelta(seconds=1)).astimezone(
        seoul
    )
    offset_equivalent["source_observed_at"] = observed_at.astimezone(seoul)
    offset_equivalent["market_evidence"] = [
        {**item, "observed_at": observed_at.astimezone(seoul)}
        for item in payload["market_evidence"]  # type: ignore[union-attr]
    ]
    assert (
        core_buy_authority_material_fingerprint(offset_equivalent)
        == command.material_fingerprint
    )


def test_block_projects_only_exact_core_v4_krx_scope() -> None:
    settings = _settings()
    engine, session_factory = _db(settings)
    now = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    matching_plan, matching = _plan_item()
    us_plan, us_item = _plan_item(exchange="NASD", currency="USD")
    paper_plan, paper_item = _plan_item(mode="paper")
    legacy_plan, legacy_item = _plan_item(core_v4=False)
    with session_factory() as db:
        db.add_all([matching_plan, us_plan, paper_plan, legacy_plan])
        db.commit()

        ack = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="block_new_buy",
                source_observed_at=now,
                evaluated_at=now,
            ),
            now=now,
        )

        assert ack.status == "applied"
        assert ack.fence_active is True
        db.refresh(matching)
        db.refresh(us_item)
        db.refresh(paper_item)
        db.refresh(legacy_item)
        assert matching.status == "watch_expired"
        assert matching.risk_context["core_buy_authority"]["action"] == "block_new_buy"
        assert us_item.status == "next_open_buy_scheduled"
        assert paper_item.status == "next_open_buy_scheduled"
        assert legacy_item.status == "next_open_buy_scheduled"
    engine.dispose()


def test_duplicate_observation_does_not_advance_recovery_streak() -> None:
    settings = _settings()
    engine, session_factory = _db(settings)
    blocked_at = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    healthy_at = blocked_at + timedelta(seconds=2)
    with session_factory() as db:
        apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="block_new_buy",
                source_observed_at=blocked_at,
                evaluated_at=blocked_at,
            ),
            now=blocked_at,
        )
        healthy = _command(
            action="allow",
            source_observed_at=healthy_at,
            evaluated_at=healthy_at,
        )
        first = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=healthy,
            now=healthy_at,
        )
        duplicate = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=healthy,
            now=healthy_at + timedelta(seconds=30),
        )

        assert first.healthy_observation_count == 1
        assert duplicate.status == "duplicate"
        assert duplicate.healthy_observation_count == 1
        assert duplicate.fence_active is True
        assert db.scalar(select(CoreBuyAuthorityObservation).where(
            CoreBuyAuthorityObservation.observation_id == healthy.observation_id
        )) is not None
    engine.dispose()


def test_same_fresh_source_with_new_observation_id_preserves_recovery_streak() -> None:
    settings = _settings()
    engine, session_factory = _db(settings)
    blocked_at = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    healthy_at = blocked_at + timedelta(seconds=2)
    with session_factory() as db:
        apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="block_new_buy",
                source_observed_at=blocked_at,
                evaluated_at=blocked_at,
            ),
            now=blocked_at,
        )
        first = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="allow",
                source_observed_at=healthy_at,
                evaluated_at=healthy_at,
            ),
            now=healthy_at,
        )
        same_source = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="allow",
                source_observed_at=healthy_at,
                evaluated_at=healthy_at + timedelta(seconds=1),
            ),
            now=healthy_at + timedelta(seconds=1),
        )

        assert first.healthy_observation_count == 1
        assert same_source.status == "applied"
        assert same_source.healthy_observation_count == 1
        assert same_source.fence_active is True
    engine.dispose()


def test_duplicate_allow_is_rejected_after_its_source_becomes_stale() -> None:
    settings = _settings(core_buy_authority_max_observation_age_seconds=60)
    engine, session_factory = _db(settings)
    observed_at = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    command = _command(
        action="allow",
        source_observed_at=observed_at,
        evaluated_at=observed_at,
    )
    with session_factory() as db:
        accepted = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=command,
            now=observed_at,
        )
        stale_duplicate = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=command,
            now=observed_at + timedelta(seconds=61),
        )

        assert accepted.fence_active is False
        assert stale_duplicate.status == "rejected_stale"
        assert stale_duplicate.fence_active is True
        state = db.scalar(select(CoreBuyAuthorityState))
        assert state is not None
        assert state.fence_active is True
        assert state.delivery_status == "rejected_stale"
    engine.dispose()


def test_regressed_source_watermark_fails_closed() -> None:
    settings = _settings()
    engine, session_factory = _db(settings)
    observed_at = datetime(2026, 9, 11, 0, 2, tzinfo=UTC)
    with session_factory() as db:
        accepted = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="allow",
                source_observed_at=observed_at,
                evaluated_at=observed_at,
            ),
            now=observed_at,
        )
        regressed = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="allow",
                source_observed_at=observed_at - timedelta(seconds=1),
                evaluated_at=observed_at + timedelta(seconds=1),
            ),
            now=observed_at + timedelta(seconds=1),
        )

        assert accepted.fence_active is False
        assert regressed.status == "rejected_stale"
        assert regressed.fence_active is True
        assert regressed.accepted_source_observed_at == observed_at
        state = db.scalar(select(CoreBuyAuthorityState))
        assert state is not None
        assert state.delivery_status == "rejected_stale"
        assert state.context["freshness_failure"] == "source_observation_regressed"
    engine.dispose()


def test_future_block_never_poison_trusted_recovery_watermark() -> None:
    settings = _settings()
    engine, session_factory = _db(settings)
    received_at = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    future_at = datetime(2099, 9, 11, 0, 1, tzinfo=UTC)
    with session_factory() as db:
        blocked = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="block_new_buy",
                source_observed_at=future_at,
                evaluated_at=future_at,
                market_date=date(2099, 9, 11),
            ),
            now=received_at,
        )
        healthy_at = received_at + timedelta(seconds=2)
        healthy = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="allow",
                source_observed_at=healthy_at,
                evaluated_at=healthy_at,
            ),
            now=healthy_at,
        )

        assert blocked.fence_active is True
        assert healthy.status == "applied"
        assert healthy.healthy_observation_count == 1
        state = db.scalar(select(CoreBuyAuthorityState))
        assert state is not None
        assert state.source_observed_at.replace(tzinfo=UTC) == healthy_at
        assert state.context["freshness_failure"] is None
    engine.dispose()


def test_recovery_rejects_allow_evidence_that_predates_latest_block() -> None:
    settings = _settings()
    engine, session_factory = _db(settings)
    base = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    with session_factory() as db:
        initial = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="allow",
                source_observed_at=base,
                evaluated_at=base,
            ),
            now=base,
        )
        assert initial.fence_active is False

        blocked_at = base + timedelta(seconds=10)
        apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="block_new_buy",
                source_observed_at=blocked_at,
                evaluated_at=blocked_at,
            ),
            now=blocked_at,
        )

        for source_offset, received_offset in ((5, 11), (7, 12)):
            rejected = apply_core_buy_authority_observation(
                settings=settings,
                db=db,
                command=_command(
                    action="allow",
                    source_observed_at=base + timedelta(seconds=source_offset),
                    evaluated_at=base + timedelta(seconds=received_offset),
                ),
                now=base + timedelta(seconds=received_offset),
            )
            assert rejected.status == "rejected_stale"
            assert rejected.fence_active is True
            assert rejected.healthy_observation_count == 0

        for source_offset in (14, 16):
            recovered = apply_core_buy_authority_observation(
                settings=settings,
                db=db,
                command=_command(
                    action="allow",
                    source_observed_at=base + timedelta(seconds=source_offset),
                    evaluated_at=base + timedelta(seconds=source_offset),
                ),
                now=base + timedelta(seconds=source_offset),
            )

        assert recovered.fence_active is False

    engine.dispose()


def test_duplicate_retries_incomplete_two_phase_projection() -> None:
    settings = _settings()
    engine, session_factory = _db(settings)
    now = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    plan, item = _plan_item(status="watch_monitoring_active")
    command = _command(
        action="block_new_buy",
        source_observed_at=now,
        evaluated_at=now,
    )
    with session_factory() as db:
        db.add(plan)
        db.commit()
        with patch(
            "app.services.core_buy_authority._project_scope",
            side_effect=RuntimeError("projection interrupted"),
        ):
            with pytest.raises(RuntimeError, match="projection interrupted"):
                apply_core_buy_authority_observation(
                    settings=settings,
                    db=db,
                    command=command,
                    now=now,
                )

        db.expire_all()
        pending = db.scalar(select(CoreBuyAuthorityState))
        assert pending is not None and pending.projection_pending is True
        assert item.status == "watch_monitoring_active"

        retried = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=command,
            now=now + timedelta(seconds=1),
        )

        assert retried.status == "duplicate"
        assert retried.projection_pending is False
        db.refresh(item)
        assert item.status == "watch_expired"
    engine.dispose()


def test_superseded_observation_cannot_project_over_newer_scope_state() -> None:
    settings = _settings()
    engine, session_factory = _db(settings)
    blocked_at = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    plan, item = _plan_item(status="watch_monitoring_active")
    stale_block = _command(
        action="block_new_buy",
        source_observed_at=blocked_at,
        evaluated_at=blocked_at,
    )
    with session_factory() as db:
        db.add(plan)
        db.commit()
        with patch(
            "app.services.core_buy_authority._project_scope",
            side_effect=RuntimeError("pause stale projection"),
        ):
            with pytest.raises(RuntimeError, match="pause stale projection"):
                apply_core_buy_authority_observation(
                    settings=settings,
                    db=db,
                    command=stale_block,
                    now=blocked_at,
                )

        newer_at = blocked_at + timedelta(seconds=2)
        newer = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="allow",
                source_observed_at=newer_at,
                evaluated_at=newer_at,
            ),
            now=newer_at,
        )
        state = db.scalar(select(CoreBuyAuthorityState))
        assert state is not None
        assert state.last_observation_id == newer.observation_id
        assert state.projection_pending is False

        changed = _project_scope(
            settings=settings,
            db=db,
            state_scope_fingerprint=state.scope_fingerprint,
            mode="block",
            command=stale_block,
            now=newer_at + timedelta(seconds=1),
        )

        assert changed == 0
        db.refresh(item)
        db.refresh(state)
        assert item.status == "watch_monitoring_active"
        assert state.last_observation_id == newer.observation_id
        assert state.projection_pending is False
    engine.dispose()


def test_distinct_fresh_observations_recover_and_restore_matching_items() -> None:
    settings = _settings()
    engine, session_factory = _db(settings)
    blocked_at = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    plan, item = _plan_item(status="watch_monitoring_active")
    with session_factory() as db:
        db.add(plan)
        db.commit()
        apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="block_new_buy",
                source_observed_at=blocked_at,
                evaluated_at=blocked_at,
            ),
            now=blocked_at,
        )
        first_at = blocked_at + timedelta(seconds=2)
        first = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="allow",
                source_observed_at=first_at,
                evaluated_at=first_at,
            ),
            now=first_at,
        )
        second_at = first_at + timedelta(seconds=2)
        second = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="allow",
                source_observed_at=second_at,
                evaluated_at=second_at,
            ),
            now=second_at,
        )

        assert first.fence_active is True
        assert second.fence_active is False
        assert second.healthy_observation_count == 2
        db.refresh(item)
        assert item.status == "watch_monitoring_active"
        assert item.risk_context["core_buy_authority"]["recovery"]["status"] == "resumed"

        reblocked_at = second_at + timedelta(seconds=2)
        apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="block_new_buy",
                source_observed_at=reblocked_at,
                evaluated_at=reblocked_at,
            ),
            now=reblocked_at,
        )
        db.refresh(item)
        assert item.status == "watch_expired"
        assert item.risk_context["core_buy_authority"]["action"] == "block_new_buy"
    engine.dispose()


def test_recovery_never_revives_an_item_expired_after_an_earlier_recovery() -> None:
    settings = _settings()
    engine, session_factory = _db(settings)
    blocked_at = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    plan, item = _plan_item(status="watch_monitoring_active")
    with session_factory() as db:
        db.add(plan)
        db.commit()
        apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="block_new_buy",
                source_observed_at=blocked_at,
                evaluated_at=blocked_at,
            ),
            now=blocked_at,
        )
        for offset in (2, 4):
            healthy_at = blocked_at + timedelta(seconds=offset)
            apply_core_buy_authority_observation(
                settings=settings,
                db=db,
                command=_command(
                    action="allow",
                    source_observed_at=healthy_at,
                    evaluated_at=healthy_at,
                ),
                now=healthy_at,
            )
        db.refresh(item)
        assert item.status == "watch_monitoring_active"

        # A separate lifecycle decision expired the watch after authority recovery.
        item.status = "watch_expired"
        db.commit()

        second_block_at = blocked_at + timedelta(seconds=6)
        apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="block_new_buy",
                source_observed_at=second_block_at,
                evaluated_at=second_block_at,
            ),
            now=second_block_at,
        )
        for offset in (8, 10):
            healthy_at = blocked_at + timedelta(seconds=offset)
            apply_core_buy_authority_observation(
                settings=settings,
                db=db,
                command=_command(
                    action="allow",
                    source_observed_at=healthy_at,
                    evaluated_at=healthy_at,
                ),
                now=healthy_at,
            )

        db.refresh(item)
        assert item.status == "watch_expired"
        assert (
            item.risk_context["core_buy_authority"]["recovery"]["status"]
            == "not_restored_terminal"
        )
    engine.dispose()


def test_authority_recovery_preserves_watch_that_expired_during_long_fence() -> None:
    settings = _settings(monitoring_watch_buy_max_age_trading_sessions=2)
    engine, session_factory = _db(settings)
    blocked_at = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    plan, item = _plan_item(status="watch_monitoring_active")
    with session_factory() as db:
        db.add_all(
            [
                plan,
                ExecutionPlan(
                    external_plan_id="later-session-one",
                    source="test-session-marker",
                    market_date=date(2026, 9, 17),
                    mode="live",
                    status="accepted",
                ),
                ExecutionPlan(
                    external_plan_id="later-session-two",
                    source="test-session-marker",
                    market_date=date(2026, 9, 18),
                    mode="live",
                    status="accepted",
                ),
            ]
        )
        db.commit()
        apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="block_new_buy",
                source_observed_at=blocked_at,
                evaluated_at=blocked_at,
            ),
            now=blocked_at,
        )
        db.refresh(item)
        assert item.status == "watch_expired"

        assert expire_prior_market_date_unfilled_watch_buys(
            settings=settings,
            db=db,
            current_market_date=date(2026, 9, 18),
            expired_at=datetime(2026, 9, 18, 1, tzinfo=UTC),
            max_age_trading_sessions=2,
        ) == 1

        for second in (2, 4):
            healthy_at = datetime(2026, 9, 18, 1, 0, second, tzinfo=UTC)
            apply_core_buy_authority_observation(
                settings=settings,
                db=db,
                command=_command(
                    action="allow",
                    source_observed_at=healthy_at,
                    evaluated_at=healthy_at,
                    market_date=date(2026, 9, 18),
                ),
                now=healthy_at,
            )

        db.refresh(item)
        assert item.status == "watch_expired"
        adjustment = item.risk_context["core_buy_authority"]
        assert adjustment["previous"]["status"] == "watch_expired"
        assert adjustment["recovery"]["status"] == "not_restored_terminal"
    engine.dispose()


def test_stale_allow_and_conflicting_same_id_fail_closed() -> None:
    settings = _settings(core_buy_authority_max_observation_age_seconds=60)
    engine, session_factory = _db(settings)
    now = datetime(2026, 9, 11, 0, 10, tzinfo=UTC)
    stale = _command(
        action="allow",
        source_observed_at=now - timedelta(seconds=61),
        evaluated_at=now,
    )
    with session_factory() as db:
        ack = apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=stale,
            now=now,
        )
        assert ack.status == "rejected_stale"
        assert ack.fence_active is True
        state = db.scalar(select(CoreBuyAuthorityState))
        assert state is not None and state.fence_active is True

        conflicting = stale.model_copy(update={"material_fingerprint": "f" * 64})
        with pytest.raises(CoreBuyAuthorityConflictError):
            apply_core_buy_authority_observation(
                settings=settings,
                db=db,
                command=conflicting,
                now=now + timedelta(seconds=1),
            )
        db.expire_all()
        state = db.scalar(select(CoreBuyAuthorityState))
        assert state is not None and state.fence_active is True
        assert state.delivery_status == "conflict"
    engine.dispose()


def test_core_v4_krx_buy_requires_current_exact_scope_authority() -> None:
    settings = _settings(core_buy_authority_max_observation_age_seconds=60)
    engine, session_factory = _db(settings)
    now = datetime(2026, 9, 11, 0, 1, tzinfo=UTC)
    plan, item = _plan_item()
    with session_factory() as db:
        db.add(plan)
        db.commit()

        assert core_buy_authority_block_reason(
            settings=settings,
            db=db,
            plan_item=item,
            now=now,
        ) == "core_buy_authority_missing"

        apply_core_buy_authority_observation(
            settings=settings,
            db=db,
            command=_command(
                action="allow",
                source_observed_at=now,
                evaluated_at=now,
            ),
            now=now,
        )
        assert core_buy_authority_block_reason(
            settings=settings,
            db=db,
            plan_item=item,
            now=now + timedelta(seconds=30),
        ) is None
        assert core_buy_authority_block_reason(
            settings=settings,
            db=db,
            plan_item=item,
            now=now + timedelta(seconds=61),
        ) == "core_buy_authority_stale"

        _, us_item = _plan_item(exchange="NASD", currency="USD")
        assert core_buy_authority_block_reason(
            settings=settings,
            db=db,
            plan_item=us_item,
            now=now + timedelta(days=1),
        ) is None
    engine.dispose()
