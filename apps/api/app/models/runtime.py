from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
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
    automation_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
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


class BrokerSession(TimestampMixin, Base):
    __tablename__ = "broker_sessions"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    api_base_url: Mapped[str | None] = mapped_column(String(255))
    token_type: Mapped[str | None] = mapped_column(String(32))
    access_token: Mapped[str | None] = mapped_column(Text, deferred=True)
    access_token_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
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
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    quantity_policy: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
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
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    broker_order: Mapped[BrokerOrder] = relationship(back_populates="events")


class ExecutionFailure(TimestampMixin, Base):
    __tablename__ = "execution_failures"
    __table_args__ = (
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
    filled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)

    broker_order: Mapped[BrokerOrder] = relationship(back_populates="fills")


class ExecutionPosition(TimestampMixin, Base):
    __tablename__ = "execution_positions"
    __table_args__ = (
        UniqueConstraint(
            "symbol",
            "mode",
            name="uq_execution_positions_symbol_mode",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    mode: Mapped[str] = mapped_column(String(16), nullable=False, default="mock")
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    base_quantity: Mapped[Decimal | None] = mapped_column(Numeric(28, 12))
    average_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    updated_from: Mapped[str] = mapped_column(String(64), nullable=False, default="bootstrap")
    last_reconciled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)


class MonitoringCheckpoint(TimestampMixin, Base):
    __tablename__ = "monitoring_checkpoints"
    __table_args__ = (
        UniqueConstraint(
            "worker_name",
            "mode",
            name="uq_monitoring_checkpoints_worker_name_mode",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    worker_name: Mapped[str] = mapped_column(String(64), nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False, default="mock")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="not_started")
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    details: Mapped[dict[str, Any] | None] = mapped_column(JSON, deferred=True)
