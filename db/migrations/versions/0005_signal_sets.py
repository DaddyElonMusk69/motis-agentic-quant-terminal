"""add signal set catalog

Revision ID: 0005_signal_sets
Revises: 0004_stage_nullable
Create Date: 2026-06-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0005_signal_sets"
down_revision = "0004_stage_nullable"
branch_labels = None
depends_on = None


def upgrade() -> None:
    json_type = postgresql.JSONB(astext_type=sa.Text())
    op.create_table(
        "signal_sets",
        sa.Column("signal_set_key", sa.String(), nullable=False),
        sa.Column("signal_set_id", sa.String(), nullable=False),
        sa.Column("signal_engine_id", sa.String(), nullable=False),
        sa.Column("signal_engine_version", sa.String(), nullable=False),
        sa.Column("asset", sa.String(), nullable=False),
        sa.Column("instrument", sa.String(), nullable=False),
        sa.Column("start_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("packet_count", sa.Integer(), nullable=False),
        sa.Column("payload_schema", sa.String(), nullable=False),
        sa.Column("source_path", sa.Text(), nullable=False),
        sa.Column("manifest", json_type, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("signal_set_key"),
        sa.UniqueConstraint("signal_engine_id", "asset", "signal_set_id"),
    )
    op.add_column("signals", sa.Column("signal_set_key", sa.String(), nullable=True))
    op.create_foreign_key(
        "signals_signal_set_key_fkey",
        "signals",
        "signal_sets",
        ["signal_set_key"],
        ["signal_set_key"],
    )


def downgrade() -> None:
    op.drop_constraint("signals_signal_set_key_fkey", "signals", type_="foreignkey")
    op.drop_column("signals", "signal_set_key")
    op.drop_table("signal_sets")
