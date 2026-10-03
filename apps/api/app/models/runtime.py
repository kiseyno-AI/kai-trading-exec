from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


def new_id() -> str:
    return str(uuid4())


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class RuntimeControlState(TimestampMixin, Base):
    __tablename__ = "runtime_control_state"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default="global")
    mode: Mapped[str] = mapped_column(String(16), nullable=False, default="mock")
    paused: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    kill_switch_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    hourly_risk_off_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )
    hourly_risk_off_epoch: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )
    hourly_risk_off_context: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        deferred=True,
    )
    readiness_reason: Mapped[str | None] = mapped_column(Text, deferred=True)
    last_transition_action: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="bootstrap",
    )
    last_transition_actor: Mapped[str | None] = mapped_column(String(128))
    last_transition_source: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default="system",
    )
    last_transition_reason: Mapped[str | None] = mapped_column(Text, deferred=True)
    last_transition_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    last_transition_event_id: Mapped[str | None] = mapped_column(String(36))

    events: Mapped[list["RuntimeControlEvent"]] = relationship(back_populates="control_state")


class RuntimeControlEvent(TimestampMixin, Base):
    __tablename__ = "runtime_control_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    runtime_control_state_id: Mapped[str] = mapped_column(
        ForeignKey("runtime_control_state.id"),
        nullable=False,
        index=True,
    )
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    outcome: Mapped[str] = mapped_column(String(16), nullable=False)
    actor: Mapped[str | None] = mapped_column(String(128))
    source: Mapped[str] = mapped_column(String(64), nullable=False, default="operator")
    reason: Mapped[str | None] = mapped_column(Text, deferred=True)
    message: Mapped[str | None] = mapped_column(Text, deferred=True)
    previous_state: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    current_state: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    control_state: Mapped[RuntimeControlState] = relationship(back_populates="events")


class CoreBuyAuthorityState(TimestampMixin, Base):
    __tablename__ = "core_buy_authority_states"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "market",
            "currency",
            "runtime_mode",
            "account_scope",
            name="uq_core_buy_authority_states_scope",
        ),
        CheckConstraint(
            "provider = 'kis' AND market = 'KRX' AND currency = 'KRW'",
            name="ck_core_buy_authority_states_krx_scope",
        ),
    )

    scope_fingerprint: Mapped[str] = mapped_column(String(64), primary_key=True)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    market: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(16), nullable=False)
    runtime_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    account_scope: Mapped[str] = mapped_column(String(64), nullable=False)
    fence_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    delivery_status: Mapped[str] = mapped_column(String(32), nullable=False)
    projection_pending: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    healthy_observation_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_observation_id: Mapped[str] = mapped_column(String(128), nullable=False)
    last_material_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    last_action: Mapped[str] = mapped_column(String(32), nullable=False)
    market_date: Mapped[date] = mapped_column(Date, nullable=False)
    source_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_healthy_source_observed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    last_block_source_observed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    last_blocked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    context: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)


class CoreBuyAuthorityObservation(TimestampMixin, Base):
    __tablename__ = "core_buy_authority_observations"
    __table_args__ = (
        Index(
            "ix_core_buy_authority_observations_scope_received",
            "scope_fingerprint",
            "received_at",
        ),
    )

    observation_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    scope_fingerprint: Mapped[str] = mapped_column(
        ForeignKey("core_buy_authority_states.scope_fingerprint"),
        nullable=False,
    )
    material_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    source_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    ack_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class BrokerSession(TimestampMixin, Base):
    __tablename__ = "broker_sessions"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    api_base_url: Mapped[str | None] = mapped_column(String(255))
    token_type: Mapped[str | None] = mapped_column(String(32))
    access_token: Mapped[str | None] = mapped_column(Text, deferred=True)
    access_token_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    generation: Mapped[str | None] = mapped_column(String(36))
    refresh_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    refresh_token: Mapped[str | None] = mapped_column(Text, deferred=True)
    refresh_token_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    approval_key: Mapped[str | None] = mapped_column(Text, deferred=True)
    approval_key_issued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_authenticated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text, deferred=True)
    last_error_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    events: Mapped[list["BrokerSessionEvent"]] = relationship(back_populates="broker_session")


class BrokerSessionEvent(TimestampMixin, Base):
    __tablename__ = "broker_session_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    broker_session_id: Mapped[str] = mapped_column(
        ForeignKey("broker_sessions.id"),
        nullable=False,
        index=True,
    )
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    outcome: Mapped[str] = mapped_column(String(16), nullable=False)
    actor: Mapped[str | None] = mapped_column(String(128))
    source: Mapped[str] = mapped_column(String(64), nullable=False, default="operator")
    reason: Mapped[str | None] = mapped_column(Text, deferred=True)
    message: Mapped[str | None] = mapped_column(Text, deferred=True)
    previous_state: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    current_state: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    broker_session: Mapped[BrokerSession] = relationship(back_populates="events")


class ExecutionPlan(TimestampMixin, Base):
    __tablename__ = "execution_plans"
    __table_args__ = (
        UniqueConstraint(
            "external_plan_id",
            "mode",
            name="uq_execution_plans_external_plan_id_mode",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    external_plan_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    market_date: Mapped[date | None] = mapped_column(Date())
    strategy_run_id: Mapped[str | None] = mapped_column(String(128))
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="accepted")
    requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    accepted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)

    items: Mapped[list["ExecutionPlanItem"]] = relationship(
        back_populates="plan",
        order_by="ExecutionPlanItem.plan_item_index",
    )


class ExecutionPlanItem(TimestampMixin, Base):
    __tablename__ = "execution_plan_items"
    __table_args__ = (
        Index(
            "ix_execution_plan_items_status_updated_at",
            "status",
            "updated_at",
        ),
        Index(
            "ix_execution_plan_items_created_at",
            "created_at",
        ),
        Index(
            "ix_execution_plan_items_side_status_intent_plan_symbol",
            "side",
            "status",
            "intent",
            "execution_plan_id",
            "symbol",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    execution_plan_id: Mapped[str] = mapped_column(
        ForeignKey("execution_plans.id"),
        nullable=False,
        index=True,
    )
    plan_item_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    intent: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(64), nullable=False, default="pending")
    quantity_policy: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    trigger_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    time_in_force: Mapped[str | None] = mapped_column(String(32))
    risk_context: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    decision_agent_name: Mapped[str | None] = mapped_column(String(128))
    decision_snapshot_id: Mapped[str | None] = mapped_column(String(128))
    decision_run_id: Mapped[str | None] = mapped_column(String(128))
    source_plan_item_id: Mapped[str | None] = mapped_column(String(128))
    source_market_date: Mapped[date | None] = mapped_column(Date())
    provider: Mapped[str | None] = mapped_column(String(32))
    venue: Mapped[str | None] = mapped_column(String(32))
    asset_class: Mapped[str | None] = mapped_column(String(32))
    base_quantity: Mapped[Decimal | None] = mapped_column(Numeric(28, 12))
    quote_notional: Mapped[Decimal | None] = mapped_column(Numeric(28, 4))
    quantity_precision: Mapped[int | None] = mapped_column(Integer)
    price_precision: Mapped[int | None] = mapped_column(Integer)

    plan: Mapped[ExecutionPlan] = relationship(back_populates="items")
    jobs: Mapped[list["ExecutionJob"]] = relationship(back_populates="plan_item")
    broker_orders: Mapped[list["BrokerOrder"]] = relationship(back_populates="plan_item")


class ExecutionPlanItemAudit(TimestampMixin, Base):
    __tablename__ = "execution_plan_item_audits"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    execution_plan_item_id: Mapped[str] = mapped_column(
        ForeignKey("execution_plan_items.id"),
        nullable=False,
        index=True,
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    summary: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    error_message: Mapped[str | None] = mapped_column(Text, deferred=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )

    plan_item: Mapped[ExecutionPlanItem] = relationship()


class ExecutionJob(TimestampMixin, Base):
    __tablename__ = "execution_jobs"
    __table_args__ = (
        UniqueConstraint(
            "execution_plan_item_id",
            "job_type",
            name="uq_execution_jobs_plan_item_job_type",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    execution_plan_item_id: Mapped[str] = mapped_column(
        ForeignKey("execution_plan_items.id"),
        nullable=False,
        index=True,
    )
    job_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    scheduled_for: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, deferred=True)

    plan_item: Mapped[ExecutionPlanItem] = relationship(back_populates="jobs")


class BrokerOrder(TimestampMixin, Base):
    __tablename__ = "broker_orders"
    __table_args__ = (
        Index(
            "ix_broker_orders_mode_submitted_at",
            "mode",
            "submitted_at",
        ),
        Index(
            "ix_broker_orders_mode_status_submitted_at",
            "mode",
            "status",
            "submitted_at",
        ),
        UniqueConstraint(
            "idempotency_key",
            "mode",
            name="uq_broker_orders_idempotency_key_mode",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    execution_plan_item_id: Mapped[str | None] = mapped_column(
        ForeignKey("execution_plan_items.id"),
        index=True,
    )
    broker_name: Mapped[str] = mapped_column(String(32), nullable=False, default="kis")
    mode: Mapped[str] = mapped_column(String(16), nullable=False, default="mock")
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    broker_order_id: Mapped[str | None] = mapped_column(String(128), index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="created")
    quantity: Mapped[int | None] = mapped_column(Integer)
    base_quantity: Mapped[Decimal | None] = mapped_column(Numeric(28, 12))
    quote_notional: Mapped[Decimal | None] = mapped_column(Numeric(28, 4))
    quantity_precision: Mapped[int | None] = mapped_column(Integer)
    price_precision: Mapped[int | None] = mapped_column(Integer)
    venue: Mapped[str | None] = mapped_column(String(32))
    asset_class: Mapped[str | None] = mapped_column(String(32))
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_broker_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    request_metadata: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    response_metadata: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    last_error_classification: Mapped[str | None] = mapped_column(String(64))
    last_error_code: Mapped[str | None] = mapped_column(String(64))
    last_error_message: Mapped[str | None] = mapped_column(Text, deferred=True)
    last_error_retryable: Mapped[bool | None] = mapped_column(Boolean)
    raw_broker_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)

    plan_item: Mapped[ExecutionPlanItem | None] = relationship(back_populates="broker_orders")
    events: Mapped[list["BrokerOrderEvent"]] = relationship(back_populates="broker_order")
    execution_failure: Mapped["ExecutionFailure | None"] = relationship(
        back_populates="broker_order",
        uselist=False,
    )
    fills: Mapped[list["ExecutionFill"]] = relationship(back_populates="broker_order")


class BrokerOrderEvent(TimestampMixin, Base):
    __tablename__ = "broker_order_events"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    broker_order_pk: Mapped[str] = mapped_column(
        ForeignKey("broker_orders.id"),
        nullable=False,
        index=True,
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    event_source: Mapped[str | None] = mapped_column(String(32))
    normalized_status: Mapped[str | None] = mapped_column(String(32))
    broker_status: Mapped[str | None] = mapped_column(String(64))
    error_classification: Mapped[str | None] = mapped_column(String(64))
    retryable: Mapped[bool | None] = mapped_column(Boolean)
    message: Mapped[str | None] = mapped_column(Text, deferred=True)
    request_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    response_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    broker_order: Mapped[BrokerOrder] = relationship(back_populates="events")


class ExecutionFailure(TimestampMixin, Base):
    __tablename__ = "execution_failures"
    __table_args__ = (
        Index(
            "ix_execution_failures_mode_latest_failed_at",
            "mode",
            "latest_failed_at",
        ),
        UniqueConstraint(
            "broker_order_pk",
            name="uq_execution_failures_broker_order_pk",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    broker_order_pk: Mapped[str] = mapped_column(
        ForeignKey("broker_orders.id"),
        nullable=False,
        index=True,
    )
    execution_plan_id: Mapped[str | None] = mapped_column(String(36), index=True)
    execution_plan_item_id: Mapped[str | None] = mapped_column(String(36), index=True)
    plan_id: Mapped[str | None] = mapped_column(String(128), index=True)
    plan_source: Mapped[str | None] = mapped_column(String(64))
    strategy_run_id: Mapped[str | None] = mapped_column(String(128))
    decision_actor: Mapped[str | None] = mapped_column(String(128))
    broker_name: Mapped[str] = mapped_column(String(32), nullable=False, default="kis")
    account: Mapped[str | None] = mapped_column(String(64))
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    intent: Mapped[str | None] = mapped_column(String(64))
    requested_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    requested_limit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    requested_notional: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    classification: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    broker_error_classification: Mapped[str | None] = mapped_column(String(64))
    broker_error_code: Mapped[str | None] = mapped_column(String(64))
    broker_error_message: Mapped[str] = mapped_column(Text, nullable=False, deferred=True)
    retryable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open", index=True)
    first_failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    latest_failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    retry_decision_status: Mapped[str | None] = mapped_column(String(32), index=True)
    retry_decision_reason: Mapped[str | None] = mapped_column(Text, deferred=True)
    retry_decision_source: Mapped[str | None] = mapped_column(String(64))
    retry_next_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    retry_deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retry_max_attempts: Mapped[int | None] = mapped_column(Integer)
    retry_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    retry_job_id: Mapped[str | None] = mapped_column(String(36), index=True)
    retry_order_idempotency_key: Mapped[str | None] = mapped_column(String(128))
    retry_exhausted_reason: Mapped[str | None] = mapped_column(Text, deferred=True)
    retry_audit_context: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    status_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status_updated_by: Mapped[str | None] = mapped_column(String(128))
    status_updated_source: Mapped[str | None] = mapped_column(String(64))
    status_updated_reason: Mapped[str | None] = mapped_column(Text, deferred=True)
    raw_context: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)

    broker_order: Mapped[BrokerOrder] = relationship(back_populates="execution_failure")


class ExecutionFill(TimestampMixin, Base):
    __tablename__ = "execution_fills"
    __table_args__ = (
        Index(
            "ix_execution_fills_filled_at_broker_order_pk",
            "filled_at",
            "broker_order_pk",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    idempotency_key: Mapped[str | None] = mapped_column(String(80), unique=True, index=True)
    broker_order_pk: Mapped[str] = mapped_column(
        ForeignKey("broker_orders.id"),
        nullable=False,
        index=True,
    )
    broker_fill_id: Mapped[str | None] = mapped_column(String(128), index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    base_quantity: Mapped[Decimal | None] = mapped_column(Numeric(28, 12))
    quote_notional: Mapped[Decimal | None] = mapped_column(Numeric(28, 4))
    price: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    settlement_currency: Mapped[str | None] = mapped_column(String(8))
    transaction_cost: Mapped[Decimal | None] = mapped_column(Numeric(28, 8))
    transaction_cost_coverage: Mapped[str | None] = mapped_column(String(16))
    transaction_cost_source: Mapped[str | None] = mapped_column(String(64))
    filled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)

    broker_order: Mapped[BrokerOrder] = relationship(back_populates="fills")


class RuntimeRemediationEvent(TimestampMixin, Base):
    __tablename__ = "runtime_remediation_events"
    __table_args__ = (
        UniqueConstraint(
            "mode",
            "candidate_type",
            "candidate_key",
            "outcome",
            name="uq_runtime_remediation_events_candidate_outcome",
        ),
        Index(
            "ix_runtime_remediation_events_mode_type_occurred",
            "mode",
            "candidate_type",
            "occurred_at",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    candidate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    candidate_key: Mapped[str] = mapped_column(String(128), nullable=False)
    subject_id: Mapped[str | None] = mapped_column(String(128), index=True)
    outcome: Mapped[str] = mapped_column(String(64), nullable=False)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False, deferred=True)
    evidence_summary: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class FifoInventoryIsolation(TimestampMixin, Base):
    __tablename__ = "fifo_inventory_isolations"
    __table_args__ = (
        UniqueConstraint(
            "mode",
            "provider",
            "account_scope",
            "venue",
            "asset_class",
            "exchange",
            "symbol",
            "broker_reconciled_at",
            name="uq_fifo_inventory_isolations_route_symbol_reconciled",
        ),
        CheckConstraint(
            "(provider IS NULL AND account_scope IS NULL AND venue IS NULL "
            "AND asset_class IS NULL AND exchange IS NULL) OR "
            "(provider IS NOT NULL AND account_scope IS NOT NULL "
            "AND venue IS NOT NULL AND asset_class IS NOT NULL "
            "AND exchange IS NOT NULL)",
            name="ck_fifo_inventory_isolations_scope_complete",
        ),
        Index(
            "ix_fifo_inventory_isolations_route_symbol_isolated_at",
            "mode",
            "provider",
            "account_scope",
            "venue",
            "asset_class",
            "exchange",
            "symbol",
            "isolated_through_at",
        ),
        Index(
            "uq_fifo_inventory_isolations_legacy_mode_symbol_reconciled",
            "mode",
            "symbol",
            "broker_reconciled_at",
            unique=True,
            sqlite_where=text(
                "provider IS NULL AND account_scope IS NULL AND venue IS NULL "
                "AND asset_class IS NULL AND exchange IS NULL"
            ),
            postgresql_where=text(
                "provider IS NULL AND account_scope IS NULL AND venue IS NULL "
                "AND asset_class IS NULL AND exchange IS NULL"
            ),
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    provider: Mapped[str | None] = mapped_column(String(32))
    account_scope: Mapped[str | None] = mapped_column(String(128))
    venue: Mapped[str | None] = mapped_column(String(32))
    asset_class: Mapped[str | None] = mapped_column(String(32))
    exchange: Mapped[str | None] = mapped_column(String(32))
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    residual_quantity: Mapped[Decimal] = mapped_column(Numeric(28, 12), nullable=False)
    last_local_fill_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    broker_position_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    broker_position_quantity: Mapped[Decimal] = mapped_column(Numeric(28, 12), nullable=False)
    broker_reconciled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    isolated_through_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    evidence_source: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default="broker_flat_reconciliation",
    )
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False, deferred=True)
    audit_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)


class ExecutionPosition(TimestampMixin, Base):
    __tablename__ = "execution_positions"
    __table_args__ = (
        UniqueConstraint(
            "mode",
            "provider",
            "account_scope",
            "exchange",
            "symbol",
            name="uq_execution_positions_route_symbol",
        ),
        CheckConstraint(
            "(provider IS NULL AND account_scope IS NULL AND exchange IS NULL) "
            "OR (provider IS NOT NULL AND account_scope IS NOT NULL "
            "AND exchange IS NOT NULL)",
            name="ck_execution_positions_scope_complete",
        ),
        Index(
            "uq_execution_positions_legacy_symbol_mode",
            "mode",
            "symbol",
            unique=True,
            sqlite_where=text(
                "provider IS NULL AND account_scope IS NULL AND exchange IS NULL"
            ),
            postgresql_where=text(
                "provider IS NULL AND account_scope IS NULL AND exchange IS NULL"
            ),
        ),
        Index(
            "ix_execution_positions_mode_route_symbol",
            "mode",
            "provider",
            "account_scope",
            "exchange",
            "symbol",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    mode: Mapped[str] = mapped_column(String(16), nullable=False, default="mock")
    provider: Mapped[str | None] = mapped_column(String(32))
    account_scope: Mapped[str | None] = mapped_column(String(128))
    exchange: Mapped[str | None] = mapped_column(String(32))
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    base_quantity: Mapped[Decimal | None] = mapped_column(Numeric(28, 12))
    average_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    updated_from: Mapped[str] = mapped_column(String(64), nullable=False, default="bootstrap")
    last_reconciled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)


class CryptoDailyBar(TimestampMixin, Base):
    __tablename__ = "crypto_daily_bars"
    __table_args__ = (
        Index(
            "ix_crypto_daily_bars_provider_symbol_close",
            "provider",
            "symbol",
            "bar_close_at",
        ),
        UniqueConstraint(
            "provider",
            "symbol",
            "timeframe",
            "bar_open_at",
            name="uq_crypto_daily_bars_provider_symbol_timeframe_bar_open",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), nullable=False, default="1Day")
    bar_open_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bar_close_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    open_price: Mapped[Decimal] = mapped_column(Numeric(28, 12), nullable=False)
    high_price: Mapped[Decimal] = mapped_column(Numeric(28, 12), nullable=False)
    low_price: Mapped[Decimal] = mapped_column(Numeric(28, 12), nullable=False)
    close_price: Mapped[Decimal] = mapped_column(Numeric(28, 12), nullable=False)
    volume: Mapped[Decimal] = mapped_column(Numeric(38, 12), nullable=False)
    trade_count: Mapped[int | None] = mapped_column(Integer)
    vwap: Mapped[Decimal | None] = mapped_column(Numeric(28, 12))
    source: Mapped[str] = mapped_column(String(64), nullable=False, default="alpaca")
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)


class CryptoStrategyState(TimestampMixin, Base):
    __tablename__ = "crypto_strategy_states"
    __table_args__ = (
        UniqueConstraint(
            "mode",
            "provider",
            "account_scope",
            "strategy_family",
            "strategy_version",
            "symbol",
            name="uq_crypto_strategy_states_scoped_symbol",
        ),
        CheckConstraint(
            "(account_scope IS NULL AND strategy_family IS NULL) OR "
            "(account_scope IS NOT NULL AND strategy_family IS NOT NULL)",
            name="ck_crypto_strategy_states_scope_complete",
        ),
        Index(
            "uq_crypto_strategy_states_legacy_mode_provider_version_symbol",
            "mode",
            "provider",
            "strategy_version",
            "symbol",
            unique=True,
            sqlite_where=text(
                "account_scope IS NULL AND strategy_family IS NULL"
            ),
            postgresql_where=text(
                "account_scope IS NULL AND strategy_family IS NULL"
            ),
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    # NULL/NULL identifies quarantined pre-scope rows. Runtime strategy code
    # only reads rows with both exact values and never auto-binds legacy state.
    account_scope: Mapped[str | None] = mapped_column(String(128))
    strategy_family: Mapped[str | None] = mapped_column(String(64))
    strategy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    last_processed_bar_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    eligible: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    target_fraction: Mapped[Decimal] = mapped_column(
        Numeric(12, 8),
        nullable=False,
        default=Decimal("0"),
    )
    strategy_owned_base_quantity: Mapped[Decimal] = mapped_column(
        Numeric(28, 12),
        nullable=False,
        default=Decimal("0"),
    )
    high_water_price: Mapped[Decimal | None] = mapped_column(Numeric(28, 12))
    protective_stop_price: Mapped[Decimal | None] = mapped_column(Numeric(28, 12))
    last_action: Mapped[str | None] = mapped_column(String(32))
    last_action_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    metrics: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)


class CryptoStrategyRiskState(TimestampMixin, Base):
    __tablename__ = "crypto_strategy_risk_states"
    __table_args__ = (
        UniqueConstraint(
            "mode",
            "provider",
            "account_scope",
            "strategy_family",
            "strategy_version",
            name="uq_crypto_strategy_risk_states_scope",
        ),
        CheckConstraint(
            "(account_scope IS NULL AND strategy_family IS NULL) OR "
            "(account_scope IS NOT NULL AND strategy_family IS NOT NULL)",
            name="ck_crypto_strategy_risk_states_scope_complete",
        ),
        Index(
            "uq_crypto_strategy_risk_states_legacy_mode_provider_version",
            "mode",
            "provider",
            "strategy_version",
            unique=True,
            sqlite_where=text(
                "account_scope IS NULL AND strategy_family IS NULL"
            ),
            postgresql_where=text(
                "account_scope IS NULL AND strategy_family IS NULL"
            ),
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    account_scope: Mapped[str | None] = mapped_column(String(128))
    strategy_family: Mapped[str | None] = mapped_column(String(64))
    strategy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    daily_baseline_equity: Mapped[Decimal | None] = mapped_column(Numeric(28, 4))
    daily_baseline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    weekly_baseline_equity: Mapped[Decimal | None] = mapped_column(Numeric(28, 4))
    weekly_baseline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    peak_equity: Mapped[Decimal | None] = mapped_column(Numeric(28, 4))
    peak_equity_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    current_equity: Mapped[Decimal | None] = mapped_column(Numeric(28, 4))
    current_equity_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    risk_status: Mapped[str] = mapped_column(String(32), nullable=False, default="healthy")
    risk_reasons: Mapped[list[str] | None] = mapped_column(JSON, deferred=True)
    metrics: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)


class CryptoStrategyDecision(Base):
    """Durable record of one strategy decision for a closed decision bar."""

    __tablename__ = "crypto_strategy_decisions"
    __table_args__ = (
        Index(
            "ix_crypto_strategy_decisions_mode_decided_at",
            "mode",
            "decided_at",
        ),
        UniqueConstraint(
            "mode",
            "provider",
            "account_scope",
            "strategy_family",
            "strategy_version",
            "symbol",
            "decision_bar_at",
            name="uq_crypto_strategy_decisions_account_family_bar",
        ),
        CheckConstraint(
            "(account_scope IS NULL AND strategy_family IS NULL) OR "
            "(account_scope IS NOT NULL AND strategy_family IS NOT NULL)",
            name="ck_crypto_strategy_decisions_scope_complete",
        ),
        Index(
            "uq_crypto_strategy_decisions_legacy_scope_bar",
            "mode",
            "provider",
            "strategy_version",
            "symbol",
            "decision_bar_at",
            unique=True,
            sqlite_where=text(
                "account_scope IS NULL AND strategy_family IS NULL"
            ),
            postgresql_where=text(
                "account_scope IS NULL AND strategy_family IS NULL"
            ),
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    account_scope: Mapped[str | None] = mapped_column(String(128))
    strategy_family: Mapped[str | None] = mapped_column(String(64))
    strategy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    decision_bar_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    decision_status: Mapped[str] = mapped_column(String(32), nullable=False, default="planned")
    eligible: Mapped[bool] = mapped_column(Boolean, nullable=False)
    target_fraction: Mapped[Decimal] = mapped_column(Numeric(12, 8), nullable=False)
    reference_price: Mapped[Decimal | None] = mapped_column(Numeric(28, 12))
    requested_base_quantity: Mapped[Decimal | None] = mapped_column(Numeric(28, 12))
    requested_quote_notional: Mapped[Decimal | None] = mapped_column(Numeric(28, 4))
    high_water_price: Mapped[Decimal | None] = mapped_column(Numeric(28, 12))
    protective_stop_price: Mapped[Decimal | None] = mapped_column(Numeric(28, 12))
    idempotency_key: Mapped[str | None] = mapped_column(String(128), index=True)
    reasons: Mapped[list[str] | None] = mapped_column(JSON, deferred=True)
    metrics: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    risk_context: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class MonitoringCheckpoint(TimestampMixin, Base):
    __tablename__ = "monitoring_checkpoints"
    __table_args__ = (
        UniqueConstraint(
            "worker_name",
            "mode",
            "provider",
            "account_scope",
            name="uq_monitoring_checkpoints_worker_mode_provider_account_scope",
        ),
        CheckConstraint(
            "(provider IS NULL AND account_scope IS NULL) OR "
            "(provider IS NOT NULL AND account_scope IS NOT NULL)",
            name="ck_monitoring_checkpoints_scope_complete",
        ),
        Index(
            "uq_monitoring_checkpoints_legacy_worker_mode",
            "worker_name",
            "mode",
            unique=True,
            sqlite_where=text("provider IS NULL AND account_scope IS NULL"),
            postgresql_where=text("provider IS NULL AND account_scope IS NULL"),
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    worker_name: Mapped[str] = mapped_column(String(64), nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False, default="mock")
    provider: Mapped[str | None] = mapped_column(String(32))
    account_scope: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="not_started")
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    details: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
