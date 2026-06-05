"""add stage1 seed strategy metadata

Revision ID: 0011_stage1_seed_meta
Revises: 0010_stage0_batch_windows
Create Date: 2026-06-04
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0011_stage1_seed_meta"
down_revision = "0010_stage0_batch_windows"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("stage1_research_sessions", sa.Column("seed_strategy_source_type", sa.String(), nullable=True))
    op.add_column("stage1_research_sessions", sa.Column("seed_strategy_source_path", sa.Text(), nullable=True))
    op.add_column("stage1_research_sessions", sa.Column("seed_strategy_source_version", sa.String(), nullable=True))
    op.add_column("stage1_research_sessions", sa.Column("seed_strategy_source_session_id", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("stage1_research_sessions", "seed_strategy_source_session_id")
    op.drop_column("stage1_research_sessions", "seed_strategy_source_version")
    op.drop_column("stage1_research_sessions", "seed_strategy_source_path")
    op.drop_column("stage1_research_sessions", "seed_strategy_source_type")
