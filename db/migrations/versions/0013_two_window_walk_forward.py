"""replace validation oos windows with walk forward

Revision ID: 0013_two_window_walk_forward
Revises: 0012_stage0_repeatable_batches
Create Date: 2026-06-05
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0013_two_window_walk_forward"
down_revision = "0012_stage0_repeatable_batches"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("stage0_universe_runs", sa.Column("walk_forward_start", sa.Date(), nullable=True))
    op.add_column("stage0_universe_runs", sa.Column("walk_forward_end", sa.Date(), nullable=True))
    op.execute("UPDATE stage0_universe_runs SET walk_forward_start = validation_start, walk_forward_end = locked_oos_end")
    op.drop_column("stage0_universe_runs", "locked_oos_end")
    op.drop_column("stage0_universe_runs", "locked_oos_start")
    op.drop_column("stage0_universe_runs", "validation_end")
    op.drop_column("stage0_universe_runs", "validation_start")

    op.add_column("stage1_research_sessions", sa.Column("walk_forward_start", sa.Date(), nullable=True))
    op.add_column("stage1_research_sessions", sa.Column("walk_forward_end", sa.Date(), nullable=True))
    op.execute("UPDATE stage1_research_sessions SET walk_forward_start = validation_start, walk_forward_end = locked_oos_end")
    op.execute(
        """
        UPDATE stage1_research_sessions
        SET manifest = (manifest - 'validation_window' - 'locked_oos_window')
            || jsonb_build_object(
                'walk_forward_window',
                jsonb_build_object(
                    'start', walk_forward_start::text,
                    'end', walk_forward_end::text
                )
            )
        WHERE manifest IS NOT NULL
        """
    )
    op.alter_column("stage1_research_sessions", "walk_forward_start", nullable=False)
    op.alter_column("stage1_research_sessions", "walk_forward_end", nullable=False)
    op.drop_column("stage1_research_sessions", "locked_oos_end")
    op.drop_column("stage1_research_sessions", "locked_oos_start")
    op.drop_column("stage1_research_sessions", "validation_end")
    op.drop_column("stage1_research_sessions", "validation_start")


def downgrade() -> None:
    op.add_column("stage1_research_sessions", sa.Column("validation_start", sa.Date(), nullable=True))
    op.add_column("stage1_research_sessions", sa.Column("validation_end", sa.Date(), nullable=True))
    op.add_column("stage1_research_sessions", sa.Column("locked_oos_start", sa.Date(), nullable=True))
    op.add_column("stage1_research_sessions", sa.Column("locked_oos_end", sa.Date(), nullable=True))
    op.execute(
        "UPDATE stage1_research_sessions SET "
        "validation_start = walk_forward_start, validation_end = walk_forward_end, "
        "locked_oos_start = walk_forward_start, locked_oos_end = walk_forward_end"
    )
    op.alter_column("stage1_research_sessions", "validation_start", nullable=False)
    op.alter_column("stage1_research_sessions", "validation_end", nullable=False)
    op.alter_column("stage1_research_sessions", "locked_oos_start", nullable=False)
    op.alter_column("stage1_research_sessions", "locked_oos_end", nullable=False)
    op.drop_column("stage1_research_sessions", "walk_forward_end")
    op.drop_column("stage1_research_sessions", "walk_forward_start")

    op.add_column("stage0_universe_runs", sa.Column("validation_start", sa.Date(), nullable=True))
    op.add_column("stage0_universe_runs", sa.Column("validation_end", sa.Date(), nullable=True))
    op.add_column("stage0_universe_runs", sa.Column("locked_oos_start", sa.Date(), nullable=True))
    op.add_column("stage0_universe_runs", sa.Column("locked_oos_end", sa.Date(), nullable=True))
    op.execute(
        "UPDATE stage0_universe_runs SET "
        "validation_start = walk_forward_start, validation_end = walk_forward_end, "
        "locked_oos_start = walk_forward_start, locked_oos_end = walk_forward_end"
    )
    op.drop_column("stage0_universe_runs", "walk_forward_end")
    op.drop_column("stage0_universe_runs", "walk_forward_start")
