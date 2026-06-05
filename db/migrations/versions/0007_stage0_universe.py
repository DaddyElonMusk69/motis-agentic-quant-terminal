"""add stage0 universe runs

Revision ID: 0007_stage0_universe
Revises: 0006_strategy_dev_runs
Create Date: 2026-06-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0007_stage0_universe"
down_revision = "0006_strategy_dev_runs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    json_type = postgresql.JSONB(astext_type=sa.Text())
    op.create_table(
        "stage0_universe_runs",
        sa.Column("universe_run_id", sa.String(), nullable=False),
        sa.Column("config_hash", sa.String(), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("forward_hours", sa.Integer(), nullable=False),
        sa.Column("trigger_rate_threshold_pct", sa.Float(), nullable=False),
        sa.Column("engine_filter", json_type, nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("summary", json_type, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("universe_run_id"),
        sa.UniqueConstraint("config_hash"),
    )
    op.create_table(
        "stage0_universe_candidates",
        sa.Column("candidate_id", sa.String(), nullable=False),
        sa.Column("universe_run_id", sa.String(), nullable=False),
        sa.Column("signal_set_key", sa.String(), nullable=False),
        sa.Column("signal_engine_id", sa.String(), nullable=False),
        sa.Column("signal_engine_version", sa.String(), nullable=False),
        sa.Column("asset", sa.String(), nullable=False),
        sa.Column("signal_set_id", sa.String(), nullable=False),
        sa.Column("packet_count", sa.Integer(), nullable=False),
        sa.Column("trigger_rate_pct", sa.Float(), nullable=True),
        sa.Column("branch_path", sa.String(), nullable=False),
        sa.Column("acceptance_status", sa.String(), nullable=False),
        sa.Column("duplicate_status", sa.String(), nullable=False),
        sa.Column("existing_strategy_id", sa.String(), nullable=True),
        sa.Column("metrics", json_type, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["signal_set_key"], ["signal_sets.signal_set_key"]),
        sa.ForeignKeyConstraint(["universe_run_id"], ["stage0_universe_runs.universe_run_id"]),
        sa.PrimaryKeyConstraint("candidate_id"),
        sa.UniqueConstraint("universe_run_id", "signal_set_key"),
    )


def downgrade() -> None:
    op.drop_table("stage0_universe_candidates")
    op.drop_table("stage0_universe_runs")
