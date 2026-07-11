"""add signal discovery sessions

Revision ID: 0029_signal_discovery_sessions
Revises: 0028_live_signal_observations
Create Date: 2026-07-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0029_signal_discovery_sessions"
down_revision = "0028_live_signal_observations"
branch_labels = None
depends_on = None


def _json_document() -> sa.types.TypeEngine:
    return postgresql.JSONB().with_variant(sa.JSON(), "sqlite")


def upgrade() -> None:
    op.create_table(
        "signal_discovery_sessions",
        sa.Column("session_id", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("asset", sa.String(), nullable=False),
        sa.Column("instrument", sa.String(), nullable=False),
        sa.Column("dataset_id", sa.String(), nullable=False),
        sa.Column("research_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("research_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("walk_forward_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("walk_forward_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("artifact_root", sa.Text(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("config", _json_document(), nullable=False),
        sa.Column("summary", _json_document(), nullable=False),
        sa.Column("frozen_target", _json_document(), nullable=False),
        sa.Column("target_version", sa.Integer(), nullable=True),
        sa.Column("candidate_engine_id", sa.String(), nullable=True),
        sa.Column("candidate_signal_set_key", sa.String(), nullable=True),
        sa.Column("evaluation", _json_document(), nullable=False),
        sa.Column("handoff", _json_document(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("session_id"),
    )
    op.create_index(
        "ix_signal_discovery_sessions_status_created",
        "signal_discovery_sessions",
        ["status", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_signal_discovery_sessions_status_created",
        table_name="signal_discovery_sessions",
    )
    op.drop_table("signal_discovery_sessions")
