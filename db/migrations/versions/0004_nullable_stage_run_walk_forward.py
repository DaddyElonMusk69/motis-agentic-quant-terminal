"""allow standalone stage runs

Revision ID: 0004_stage_nullable
Revises: 0003_backtest_runs
Create Date: 2026-06-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0004_stage_nullable"
down_revision = "0003_backtest_runs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("stage_runs", "walk_forward_run_id", existing_type=sa.String(), nullable=True)


def downgrade() -> None:
    op.alter_column("stage_runs", "walk_forward_run_id", existing_type=sa.String(), nullable=False)
