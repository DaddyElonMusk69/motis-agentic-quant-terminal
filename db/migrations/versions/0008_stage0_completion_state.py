"""add stage0 completion state

Revision ID: 0008_stage0_completion_state
Revises: 0007_stage0_universe
Create Date: 2026-06-04
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0008_stage0_completion_state"
down_revision = "0007_stage0_universe"
branch_labels = None
depends_on = None


def upgrade() -> None:
    json_type = postgresql.JSONB(astext_type=sa.Text())
    op.add_column(
        "stage0_universe_candidates",
        sa.Column("last_error", json_type, nullable=False, server_default=sa.text("'{}'::jsonb")),
    )
    op.alter_column("stage0_universe_candidates", "last_error", server_default=None)


def downgrade() -> None:
    op.drop_column("stage0_universe_candidates", "last_error")
