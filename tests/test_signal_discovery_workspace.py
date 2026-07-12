from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from quant_terminal_worker.signal_discovery.workspace import (
    TrainingAtlasWorkspace,
    discovery_artifact_root,
    freeze_target_contract,
    materialize_training_atlas,
    read_frozen_target,
    write_session_manifest,
)


def test_training_atlas_workspace_checkpoints_and_reloads_one_risk(tmp_path: Path) -> None:
    artifact_root = discovery_artifact_root(
        workspace_root=tmp_path,
        session_id="btc-batched-atlas",
    )
    run_identity = {"evidence_manifest_hash": "abc123", "risk_values": [1.0, 1.5]}
    workspace = TrainingAtlasWorkspace(
        artifact_root=artifact_root,
        run_identity=run_identity,
    )

    workspace.write_risk_partition(
        risk_index=0,
        risk_pct=1.0,
        timestamp_labels=[
            {
                "decision_ts": _ts("2026-01-01T00:00:00Z"),
                "label": "LONG",
                "risk_pct": 1.0,
            }
        ],
        episodes=[
            {
                "episode_id": "episode-000001",
                "direction": "LONG",
                "risk_pct": 1.0,
            }
        ],
        hard_negatives=[],
        r_summary={"risk_pct": 1.0, "primary": {"episode_count": 1}},
        purged_decision_count=3,
    )

    resumed = TrainingAtlasWorkspace(
        artifact_root=artifact_root,
        run_identity=run_identity,
    )

    assert resumed.completed_risk_values == (1.0,)
    assert resumed.completed_episode_count == 1
    assert resumed.r_summaries == [
        {"risk_pct": 1.0, "primary": {"episode_count": 1}}
    ]
    checkpoint = json.loads((artifact_root / "atlas/checkpoint.json").read_text())
    assert checkpoint["schema_version"] == "signal_discovery_atlas_checkpoint.v1"
    assert checkpoint["completed_risks"][0]["timestamp_label_count"] == 1


def test_training_atlas_workspace_rejects_identity_or_part_drift(tmp_path: Path) -> None:
    artifact_root = tmp_path / "atlas-session"
    workspace = TrainingAtlasWorkspace(
        artifact_root=artifact_root,
        run_identity={"evidence_manifest_hash": "original"},
    )
    workspace.write_risk_partition(
        risk_index=0,
        risk_pct=1.0,
        timestamp_labels=[{"decision_ts": _ts("2026-01-01T00:00:00Z"), "label": "NEUTRAL"}],
        episodes=[],
        hard_negatives=[],
        r_summary={"risk_pct": 1.0},
        purged_decision_count=0,
    )

    with pytest.raises(ValueError, match="run identity"):
        TrainingAtlasWorkspace(
            artifact_root=artifact_root,
            run_identity={"evidence_manifest_hash": "changed"},
        )

    label_part = next((artifact_root / "atlas/.work").glob("risk-*/timestamp_labels.parquet"))
    label_part.write_bytes(label_part.read_bytes() + b"drift")
    with pytest.raises(ValueError, match="fingerprint"):
        TrainingAtlasWorkspace(
            artifact_root=artifact_root,
            run_identity={"evidence_manifest_hash": "original"},
        )


def test_training_atlas_workspace_streams_parts_to_legacy_final_paths(tmp_path: Path) -> None:
    artifact_root = tmp_path / "atlas-session"
    workspace = TrainingAtlasWorkspace(
        artifact_root=artifact_root,
        run_identity={"evidence_manifest_hash": "abc123", "risk_values": [1.0, 1.5]},
    )
    for risk_index, risk_pct in enumerate((1.0, 1.5)):
        workspace.write_risk_partition(
            risk_index=risk_index,
            risk_pct=risk_pct,
            timestamp_labels=[
                {
                    "decision_ts": _ts("2026-01-01T00:00:00Z"),
                    "label": "LONG",
                    "risk_pct": risk_pct,
                    "first_touch_ts": (
                        None
                        if risk_index == 0
                        else _ts("2026-01-01T00:05:00Z")
                    ),
                    "long": {
                        "outcome": "TIMEOUT" if risk_index == 0 else "TP",
                        "first_touch_ts": (
                            None
                            if risk_index == 0
                            else _ts("2026-01-01T00:05:00Z")
                        ),
                    },
                }
            ],
            episodes=[
                {
                    "episode_id": f"episode-{risk_index + 1:06d}",
                    "direction": "LONG",
                    "risk_pct": risk_pct,
                }
            ],
            hard_negatives=[
                {
                    "decision_ts": _ts("2026-01-02T00:00:00Z"),
                    "risk_pct": risk_pct,
                }
            ],
            r_summary={"risk_pct": risk_pct},
            purged_decision_count=2,
        )

    paths = workspace.finalize(
        features=[
            {
                "decision_ts": _ts("2026-01-01T00:00:00Z"),
                "return_4h_pct": 1.25,
            }
        ],
        r_feasibility={"r_summaries": workspace.r_summaries},
    )

    assert set(paths) == {
        "training_timestamp_labels",
        "training_episodes",
        "training_features",
        "training_hard_negatives",
        "r_feasibility",
    }
    assert [
        row["risk_pct"]
        for row in pq.read_table(paths["training_timestamp_labels"]).to_pylist()
    ] == [1.0, 1.5]
    assert [
        row["episode_id"]
        for row in pq.read_table(paths["training_episodes"]).to_pylist()
    ] == ["episode-000001", "episode-000002"]
    assert paths["training_timestamp_labels"] == (
        artifact_root / "atlas/training_timestamp_labels.parquet"
    )


def test_training_atlas_materializes_the_complete_training_only_layout(tmp_path: Path) -> None:
    artifact_root = discovery_artifact_root(
        workspace_root=tmp_path,
        session_id="btc-fixed-r-v1",
    )

    paths = materialize_training_atlas(
        artifact_root=artifact_root,
        timestamp_labels=[
            {
                "decision_ts": _ts("2026-01-01T00:00:00Z"),
                "label": "LONG",
                "risk_pct": 1.0,
            }
        ],
        episodes=[
            {
                "episode_id": "episode-000001",
                "direction": "LONG",
                "start_ts": _ts("2026-01-01T00:00:00Z"),
                "end_ts": _ts("2026-01-01T00:05:00Z"),
                "timestamp_count": 2,
            }
        ],
        features=[
            {
                "decision_ts": _ts("2026-01-01T00:00:00Z"),
                "return_4h_pct": 1.25,
            }
        ],
        hard_negatives=[
            {
                "decision_ts": _ts("2026-01-02T00:00:00Z"),
                "matched_episode_id": "episode-000001",
            }
        ],
        r_feasibility={"r_summaries": [{"risk_pct": 1.0, "episode_count": 1}]},
    )
    manifest = write_session_manifest(
        artifact_root=artifact_root,
        manifest={"session_id": "btc-fixed-r-v1", "status": "atlas_ready"},
    )

    assert set(paths) == {
        "training_timestamp_labels",
        "training_episodes",
        "training_features",
        "training_hard_negatives",
        "r_feasibility",
    }
    assert (artifact_root / "manifest.json").is_file()
    assert (artifact_root / "atlas/training_timestamp_labels.parquet").is_file()
    assert (artifact_root / "atlas/training_episodes.parquet").is_file()
    assert (artifact_root / "atlas/training_features.parquet").is_file()
    assert (artifact_root / "atlas/training_hard_negatives.parquet").is_file()
    assert (artifact_root / "atlas/r_feasibility.json").is_file()
    assert not list(artifact_root.rglob("*walk_forward*"))
    assert pq.read_table(paths["training_timestamp_labels"]).to_pylist()[0]["label"] == "LONG"
    assert json.loads(paths["r_feasibility"].read_text())["r_summaries"][0][
        "risk_pct"
    ] == 1.0
    assert manifest["schema_version"] == "signal_discovery_session.v1"


def test_frozen_target_is_versioned_hashed_idempotent_and_immutable(tmp_path: Path) -> None:
    artifact_root = discovery_artifact_root(
        workspace_root=tmp_path,
        session_id="btc-fixed-r-v1",
    )
    materialize_training_atlas(
        artifact_root=artifact_root,
        timestamp_labels=[],
        episodes=[],
        features=[],
        hard_negatives=[],
        r_feasibility={"r_summaries": [{"risk_pct": 1.0}]},
    )
    selected_target = {
        "selected_risk_pct": 1.0,
        "reward_multiple": 1.5,
        "stop_multiple": 1.0,
        "horizon_hours": 72,
        "entry_delay_minutes": 5,
        "entry_semantics": "next_5m_open",
        "fee_bps_per_side": 5.0,
        "slippage_bps_per_side": 5.0,
    }
    source_data = {
        "dataset_id": "okx-btc-raw-5m-v7",
        "storage_backend": "parquet",
        "storage_uri": ".data/market-data/origin=raw/source=okx/type=candles/asset=BTC/timeframe=5m",
        "timeframe": "5m",
    }
    splits = {
        "research_start": "2025-03-01T00:00:00Z",
        "research_end": "2026-03-31T23:55:00Z",
        "walk_forward_start": "2026-04-01T00:00:00Z",
        "walk_forward_end": "2026-05-30T23:55:00Z",
    }

    bracket_contract = {
        "schema_version": "signal_discovery_bracket_policy.v1",
        "policy_hash": "policy-hash",
        "training_brackets_path": "brackets/training_brackets.parquet",
        "training_brackets_hash": "brackets-hash",
    }
    contract = freeze_target_contract(
        artifact_root=artifact_root,
        session_id="btc-fixed-r-v1",
        selected_target=selected_target,
        source_data=source_data,
        splits=splits,
        bracket_contract=bracket_contract,
    )
    same_contract = freeze_target_contract(
        artifact_root=artifact_root,
        session_id="btc-fixed-r-v1",
        selected_target=selected_target,
        source_data=source_data,
        splits=splits,
        bracket_contract=bracket_contract,
    )

    assert contract == same_contract == read_frozen_target(artifact_root=artifact_root)
    assert contract["schema_version"] == "signal_discovery_target.v1"
    assert contract["target_version"] == 1
    assert contract["source_data"]["dataset_id"] == "okx-btc-raw-5m-v7"
    assert contract["selected_target"]["reward_multiple"] == 1.5
    assert contract["bracket_policy"]["policy_hash"] == "policy-hash"
    assert contract["splits"]["walk_forward_start"] == "2026-04-01T00:00:00Z"
    assert len(contract["config_hash"]) == 64
    assert (artifact_root / "target/frozen_target.json").is_file()

    with pytest.raises(ValueError, match="immutable"):
        freeze_target_contract(
            artifact_root=artifact_root,
            session_id="btc-fixed-r-v1",
            selected_target={**selected_target, "selected_risk_pct": 1.25},
            source_data=source_data,
            splits=splits,
        )


def test_freeze_requires_a_materialized_training_atlas(tmp_path: Path) -> None:
    artifact_root = discovery_artifact_root(
        workspace_root=tmp_path,
        session_id="btc-fixed-r-v1",
    )

    with pytest.raises(ValueError, match="training atlas"):
        freeze_target_contract(
            artifact_root=artifact_root,
            session_id="btc-fixed-r-v1",
            selected_target={
                "selected_risk_pct": 1.0,
                "reward_multiple": 2.0,
                "stop_multiple": 1.0,
                "horizon_hours": 36,
                "entry_delay_minutes": 5,
                "entry_semantics": "next_5m_open",
            },
            source_data={"dataset_id": "btc", "storage_backend": "parquet"},
            splits={
                "research_start": "2025-01-01T00:00:00Z",
                "research_end": "2025-12-31T00:00:00Z",
                "walk_forward_start": "2026-01-01T00:00:00Z",
                "walk_forward_end": "2026-02-01T00:00:00Z",
            },
        )


def test_frozen_target_rejects_nonpositive_reward_multiple(tmp_path: Path) -> None:
    artifact_root = discovery_artifact_root(
        workspace_root=tmp_path,
        session_id="btc-invalid-reward-v1",
    )
    materialize_training_atlas(
        artifact_root=artifact_root,
        timestamp_labels=[],
        episodes=[],
        features=[],
        hard_negatives=[],
        r_feasibility={"r_summaries": [{"risk_pct": 1.0}]},
    )

    with pytest.raises(ValueError, match="reward_multiple must be positive"):
        freeze_target_contract(
            artifact_root=artifact_root,
            session_id="btc-invalid-reward-v1",
            selected_target={
                "selected_risk_pct": 1.0,
                "reward_multiple": 0.0,
                "stop_multiple": 1.0,
                "horizon_hours": 36,
                "entry_delay_minutes": 5,
                "entry_semantics": "next_5m_open",
            },
            source_data={"dataset_id": "btc", "storage_backend": "parquet"},
            splits={
                "research_start": "2025-01-01T00:00:00Z",
                "research_end": "2025-12-31T00:00:00Z",
                "walk_forward_start": "2026-01-01T00:00:00Z",
                "walk_forward_end": "2026-02-01T00:00:00Z",
            },
        )


def test_discovery_artifact_root_rejects_path_traversal(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="session_id"):
        discovery_artifact_root(workspace_root=tmp_path, session_id="../escape")


def _ts(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC)
