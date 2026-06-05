"""add stage0 batch split windows

Revision ID: 0010_stage0_batch_windows
Revises: 0009_stage1_research_sessions
Create Date: 2026-06-04
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0010_stage0_batch_windows"
down_revision = "0009_stage1_research_sessions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("stage0_universe_runs", sa.Column("train_start", sa.Date(), nullable=True))
    op.add_column("stage0_universe_runs", sa.Column("train_end", sa.Date(), nullable=True))
    op.add_column("stage0_universe_runs", sa.Column("validation_start", sa.Date(), nullable=True))
    op.add_column("stage0_universe_runs", sa.Column("validation_end", sa.Date(), nullable=True))
    op.add_column("stage0_universe_runs", sa.Column("locked_oos_start", sa.Date(), nullable=True))
    op.add_column("stage0_universe_runs", sa.Column("locked_oos_end", sa.Date(), nullable=True))


def downgrade() -> None:
    op.drop_column("stage0_universe_runs", "locked_oos_end")
    op.drop_column("stage0_universe_runs", "locked_oos_start")
    op.drop_column("stage0_universe_runs", "validation_end")
    op.drop_column("stage0_universe_runs", "validation_start")
    op.drop_column("stage0_universe_runs", "train_end")
    op.drop_column("stage0_universe_runs", "train_start")
