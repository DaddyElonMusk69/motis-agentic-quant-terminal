"""add strategy development runs

Revision ID: 0006_strategy_dev_runs
Revises: 0005_signal_sets
Create Date: 2026-06-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0006_strategy_dev_runs"
down_revision = "0005_signal_sets"
branch_labels = None
depends_on = None


def upgrade() -> None:
    json_type = postgresql.JSONB(astext_type=sa.Text())
    op.create_table(
        "strategy_development_runs",
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("stage", sa.String(), nullable=False),
        sa.Column("strategy_id", sa.String(), nullable=False),
        sa.Column("strategy_version", sa.String(), nullable=False),
        sa.Column("signal_engine_id", sa.String(), nullable=False),
        sa.Column("signal_engine_version", sa.String(), nullable=False),
        sa.Column("signal_set_key", sa.String(), nullable=False),
        sa.Column("asset", sa.String(), nullable=False),
        sa.Column("forward_hours", sa.Integer(), nullable=False),
        sa.Column("significance_threshold_pct", sa.Float(), nullable=True),
        sa.Column("artifact_root", sa.Text(), nullable=False),
        sa.Column("commands", json_type, nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("metrics", json_type, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["signal_set_key"], ["signal_sets.signal_set_key"]),
        sa.PrimaryKeyConstraint("run_id"),
    )


def downgrade() -> None:
    op.drop_table("strategy_development_runs")
