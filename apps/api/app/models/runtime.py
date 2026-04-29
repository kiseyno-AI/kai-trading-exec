from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, JSON, Numeric, String, Text, func
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
    readiness_reason: Mapped[str | None] = mapped_column(Text)


class ExecutionPlan(TimestampMixin, Base):
    __tablename__ = "execution_plans"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    external_plan_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="accepted")
    requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    accepted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    items: Mapped[list["ExecutionPlanItem"]] = relationship(back_populates="plan")


class ExecutionPlanItem(TimestampMixin, Base):
    __tablename__ = "execution_plan_items"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    execution_plan_id: Mapped[str] = mapped_column(
        ForeignKey("execution_plans.id"),
        nullable=False,
        index=True,
    )
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    intent: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    quantity_policy: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    risk_context: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    decision_snapshot_id: Mapped[str | None] = mapped_column(String(128))

    plan: Mapped[ExecutionPlan] = relationship(back_populates="items")
    jobs: Mapped[list["ExecutionJob"]] = relationship(back_populates="plan_item")
    broker_orders: Mapped[list["BrokerOrder"]] = relationship(back_populates="plan_item")


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
    last_error: Mapped[str | None] = mapped_column(Text)

    plan_item: Mapped[ExecutionPlanItem] = relationship(back_populates="jobs")


class BrokerOrder(TimestampMixin, Base):
    __tablename__ = "broker_orders"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    execution_plan_item_id: Mapped[str | None] = mapped_column(
        ForeignKey("execution_plan_items.id"),
        index=True,
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    broker_order_id: Mapped[str | None] = mapped_column(String(128), index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="created")
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_broker_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    raw_broker_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    plan_item: Mapped[ExecutionPlanItem | None] = relationship(back_populates="broker_orders")
    events: Mapped[list["BrokerOrderEvent"]] = relationship(back_populates="broker_order")
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
    broker_status: Mapped[str | None] = mapped_column(String(64))
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    broker_order: Mapped[BrokerOrder] = relationship(back_populates="events")


class ExecutionFill(TimestampMixin, Base):
    __tablename__ = "execution_fills"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    broker_order_pk: Mapped[str] = mapped_column(
        ForeignKey("broker_orders.id"),
        nullable=False,
        index=True,
    )
    broker_fill_id: Mapped[str | None] = mapped_column(String(128), index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    filled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    broker_order: Mapped[BrokerOrder] = relationship(back_populates="fills")


class ExecutionPosition(TimestampMixin, Base):
    __tablename__ = "execution_positions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    average_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    updated_from: Mapped[str] = mapped_column(String(64), nullable=False, default="bootstrap")
    last_reconciled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class MonitoringCheckpoint(TimestampMixin, Base):
    __tablename__ = "monitoring_checkpoints"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    worker_name: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="not_started")
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    details: Mapped[dict[str, Any] | None] = mapped_column(JSON)

