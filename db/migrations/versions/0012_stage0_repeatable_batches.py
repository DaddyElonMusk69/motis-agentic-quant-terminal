"""allow repeatable stage0 batches

Revision ID: 0012_stage0_repeatable_batches
Revises: 0011_stage1_seed_meta
Create Date: 2026-06-04
"""

from __future__ import annotations

from alembic import op

revision = "0012_stage0_repeatable_batches"
down_revision = "0011_stage1_seed_meta"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("stage0_universe_runs_config_hash_key", "stage0_universe_runs", type_="unique")


def downgrade() -> None:
    op.create_unique_constraint(
        "stage0_universe_runs_config_hash_key",
        "stage0_universe_runs",
        ["config_hash"],
    )
