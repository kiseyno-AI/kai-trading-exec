"""Add durable Alpaca crypto strategy state.

Revision ID: 202608160001
Revises: 202608010001
Create Date: 2026-08-16 12:00:00.000000
"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "202608160001"
down_revision: str | None = "202608010001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "crypto_daily_bars",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("timeframe", sa.String(length=16), nullable=False),
        sa.Column("bar_open_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bar_close_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open_price", sa.Numeric(28, 12), nullable=False),
        sa.Column("high_price", sa.Numeric(28, 12), nullable=False),
        sa.Column("low_price", sa.Numeric(28, 12), nullable=False),
        sa.Column("close_price", sa.Numeric(28, 12), nullable=False),
        sa.Column("volume", sa.Numeric(38, 12), nullable=False),
        sa.Column("trade_count", sa.Integer(), nullable=True),
        sa.Column("vwap", sa.Numeric(28, 12), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False),
        sa.Column("raw_payload", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_crypto_daily_bars")),
        sa.UniqueConstraint(
            "provider",
            "symbol",
            "timeframe",
            "bar_open_at",
            name="uq_crypto_daily_bars_provider_symbol_timeframe_bar_open",
        ),
    )
    op.create_index(
        "ix_crypto_daily_bars_provider_symbol_close",
        "crypto_daily_bars",
        ["provider", "symbol", "bar_close_at"],
        unique=False,
    )

    op.create_table(
        "crypto_strategy_states",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("strategy_version", sa.String(length=64), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("last_processed_bar_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("eligible", sa.Boolean(), nullable=False),
        sa.Column("target_fraction", sa.Numeric(12, 8), nullable=False),
        sa.Column("high_water_price", sa.Numeric(28, 12), nullable=True),
        sa.Column("protective_stop_price", sa.Numeric(28, 12), nullable=True),
        sa.Column("last_action", sa.String(length=32), nullable=True),
        sa.Column("last_action_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metrics", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_crypto_strategy_states")),
        sa.UniqueConstraint(
            "mode",
            "provider",
            "strategy_version",
            "symbol",
            name="uq_crypto_strategy_states_mode_provider_version_symbol",
        ),
    )

    op.create_table(
        "crypto_strategy_risk_states",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("strategy_version", sa.String(length=64), nullable=False),
        sa.Column("daily_baseline_equity", sa.Numeric(28, 4), nullable=True),
        sa.Column("daily_baseline_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("weekly_baseline_equity", sa.Numeric(28, 4), nullable=True),
        sa.Column("weekly_baseline_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("peak_equity", sa.Numeric(28, 4), nullable=True),
        sa.Column("peak_equity_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("current_equity", sa.Numeric(28, 4), nullable=True),
        sa.Column("current_equity_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("risk_status", sa.String(length=32), nullable=False),
        sa.Column("risk_reasons", sa.JSON(), nullable=True),
        sa.Column("metrics", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_crypto_strategy_risk_states")),
        sa.UniqueConstraint(
            "mode",
            "provider",
            "strategy_version",
            name="uq_crypto_strategy_risk_states_mode_provider_version",
        ),
    )

    op.create_table(
        "crypto_strategy_decisions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("mode", sa.String(length=16), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("strategy_version", sa.String(length=64), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("decision_bar_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "decided_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("decision_status", sa.String(length=32), nullable=False),
        sa.Column("eligible", sa.Boolean(), nullable=False),
        sa.Column("target_fraction", sa.Numeric(12, 8), nullable=False),
        sa.Column("reference_price", sa.Numeric(28, 12), nullable=True),
        sa.Column("requested_base_quantity", sa.Numeric(28, 12), nullable=True),
        sa.Column("requested_quote_notional", sa.Numeric(28, 4), nullable=True),
        sa.Column("high_water_price", sa.Numeric(28, 12), nullable=True),
        sa.Column("protective_stop_price", sa.Numeric(28, 12), nullable=True),
        sa.Column("idempotency_key", sa.String(length=128), nullable=True),
        sa.Column("reasons", sa.JSON(), nullable=True),
        sa.Column("metrics", sa.JSON(), nullable=True),
        sa.Column("risk_context", sa.JSON(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_crypto_strategy_decisions")),
        sa.UniqueConstraint(
            "mode",
            "provider",
            "strategy_version",
            "symbol",
            "decision_bar_at",
            name="uq_crypto_strategy_decisions_scope_bar",
        ),
    )
    op.create_index(
        "ix_crypto_strategy_decisions_mode_decided_at",
        "crypto_strategy_decisions",
        ["mode", "decided_at"],
        unique=False,
    )
    op.create_index(
        op.f("ix_crypto_strategy_decisions_idempotency_key"),
        "crypto_strategy_decisions",
        ["idempotency_key"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_crypto_strategy_decisions_idempotency_key"),
        table_name="crypto_strategy_decisions",
    )
    op.drop_index(
        "ix_crypto_strategy_decisions_mode_decided_at",
        table_name="crypto_strategy_decisions",
    )
    op.drop_table("crypto_strategy_decisions")
    op.drop_table("crypto_strategy_risk_states")
    op.drop_table("crypto_strategy_states")
    op.drop_index(
        "ix_crypto_daily_bars_provider_symbol_close",
        table_name="crypto_daily_bars",
    )
    op.drop_table("crypto_daily_bars")
