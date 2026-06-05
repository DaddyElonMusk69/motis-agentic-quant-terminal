"""add stage1 research sessions

Revision ID: 0009_stage1_research_sessions
Revises: 0008_stage0_completion_state
Create Date: 2026-06-04
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0009_stage1_research_sessions"
down_revision = "0008_stage0_completion_state"
branch_labels = None
depends_on = None


def upgrade() -> None:
    json_type = postgresql.JSONB(astext_type=sa.Text())
    op.create_table(
        "stage1_research_sessions",
        sa.Column("session_id", sa.String(), nullable=False),
        sa.Column("source_universe_run_id", sa.String(), nullable=False),
        sa.Column("source_candidate_id", sa.String(), nullable=False),
        sa.Column("signal_set_key", sa.String(), nullable=False),
        sa.Column("signal_engine_id", sa.String(), nullable=False),
        sa.Column("signal_engine_version", sa.String(), nullable=False),
        sa.Column("asset", sa.String(), nullable=False),
        sa.Column("signal_set_id", sa.String(), nullable=False),
        sa.Column("strategy_id", sa.String(), nullable=False),
        sa.Column("strategy_version", sa.String(), nullable=False),
        sa.Column("train_start", sa.Date(), nullable=False),
        sa.Column("train_end", sa.Date(), nullable=False),
        sa.Column("validation_start", sa.Date(), nullable=False),
        sa.Column("validation_end", sa.Date(), nullable=False),
        sa.Column("locked_oos_start", sa.Date(), nullable=False),
        sa.Column("locked_oos_end", sa.Date(), nullable=False),
        sa.Column("artifact_root", sa.Text(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("manifest", json_type, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["signal_set_key"], ["signal_sets.signal_set_key"]),
        sa.ForeignKeyConstraint(["source_candidate_id"], ["stage0_universe_candidates.candidate_id"]),
        sa.ForeignKeyConstraint(["source_universe_run_id"], ["stage0_universe_runs.universe_run_id"]),
        sa.PrimaryKeyConstraint("session_id"),
        sa.UniqueConstraint("source_candidate_id", "strategy_id", "strategy_version"),
    )


def downgrade() -> None:
    op.drop_table("stage1_research_sessions")
