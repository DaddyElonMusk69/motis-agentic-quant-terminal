"""add live signal observations

Revision ID: 0028_live_signal_observations
Revises: 0027_worker_heartbeats
Create Date: 2026-06-23
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0028_live_signal_observations"
down_revision = "0027_worker_heartbeats"
branch_labels = None
depends_on = None


def _json_document() -> sa.types.TypeEngine:
    return postgresql.JSONB().with_variant(sa.JSON(), "sqlite")


def upgrade() -> None:
    op.create_table(
        "live_signal_observations",
        sa.Column("observation_id", sa.String(), nullable=False),
        sa.Column("signal_engine_id", sa.String(), nullable=False),
        sa.Column("signal_engine_version", sa.String(), nullable=False),
        sa.Column("asset", sa.String(), nullable=False),
        sa.Column("instrument", sa.String(), nullable=False),
        sa.Column("signal_id", sa.String(), nullable=False),
        sa.Column("signal_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("route_id", sa.String(), nullable=True),
        sa.Column("bundle_id", sa.String(), nullable=True),
        sa.Column("packet_hash", sa.String(), nullable=False),
        sa.Column("payload_schema", sa.String(), nullable=False),
        sa.Column("payload", _json_document(), nullable=False),
        sa.Column("decision", _json_document(), nullable=False),
        sa.Column("scan_metadata", _json_document(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("observation_id"),
        sa.UniqueConstraint(
            "signal_engine_id",
            "asset",
            "signal_timestamp",
            "route_id",
            name="uq_live_signal_observation_engine_asset_ts_route",
        ),
    )
    op.create_index(
        "ix_live_signal_observations_engine_asset_ts",
        "live_signal_observations",
        ["signal_engine_id", "asset", "signal_timestamp"],
    )
    op.create_index("ix_live_signal_observations_observed_at", "live_signal_observations", ["observed_at"])


def downgrade() -> None:
    op.drop_index("ix_live_signal_observations_observed_at", table_name="live_signal_observations")
    op.drop_index("ix_live_signal_observations_engine_asset_ts", table_name="live_signal_observations")
    op.drop_table("live_signal_observations")
