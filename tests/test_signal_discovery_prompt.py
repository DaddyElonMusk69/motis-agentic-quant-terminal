from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from quant_terminal_worker.signal_discovery.prompt import generate_engine_builder_prompt
from quant_terminal_worker.signal_discovery.workspace import freeze_target_contract


def test_engine_builder_prompt_is_deterministic_training_only_and_actionable(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "dev/signal_discovery_sessions/btc-fixed-r-v1"
    atlas_root = artifact_root / "atlas"
    atlas_root.mkdir(parents=True)
    exact_opportunity_timestamp = "2025-08-19T13:35:00Z"
    exact_episode_id = "episode-secret-0007"
    rows_by_name = {
        "training_timestamp_labels.parquet": [
            {
                "decision_ts": exact_opportunity_timestamp,
                "label": "LONG",
                "episode_id": exact_episode_id,
            }
        ],
        "training_episodes.parquet": [
            {
                "episode_id": exact_episode_id,
                "direction": "LONG",
                "start_ts": exact_opportunity_timestamp,
            }
        ],
        "training_features.parquet": [
            {"decision_ts": exact_opportunity_timestamp, "return_4h_pct": 1.25}
        ],
        "training_hard_negatives.parquet": [
            {"decision_ts": "2025-08-20T07:10:00Z", "matched_episode_id": exact_episode_id}
        ],
    }
    for name, rows in rows_by_name.items():
        pq.write_table(pa.Table.from_pylist(rows), atlas_root / name)
    (atlas_root / "r_feasibility.json").write_text(
        '{"r_summaries":[{"risk_pct":1.0}]}\n'
    )
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
        source_data={
            "dataset_id": "okx-btc-raw-5m-v7",
            "storage_backend": "parquet",
            "storage_uri": ".data/market-data/btc-5m",
        },
        splits={
            "research_start": "2025-03-01T00:00:00Z",
            "research_end": "2026-03-31T23:55:00Z",
            "walk_forward_start": "2026-04-01T00:00:00Z",
            "walk_forward_end": "2026-05-30T23:55:00Z",
        },
    )

    first = generate_engine_builder_prompt(
        workspace_root=tmp_path,
        artifact_root=artifact_root,
    )
    second = generate_engine_builder_prompt(
        workspace_root=tmp_path,
        artifact_root=artifact_root,
    )

    assert first == second
    prompt = first["prompt"]
    assert Path(first["prompt_path"]).read_text() == prompt
    assert first["prompt_type"] == "signal_discovery_engine_builder"
    assert "$signal-engine-builder" in prompt
    for name in rows_by_name:
        assert str(atlas_root / name) in prompt
    assert str(artifact_root / "target/frozen_target.json") in prompt
    assert str(tmp_path / "artifacts/signal_engine/engine_registry.json") in prompt
    assert str(tmp_path / "apps/worker/src/quant_terminal_worker/signal_engines") in prompt
    assert str(
        tmp_path / "packages/strategy_modules/src/quant_terminal_strategies"
    ) in prompt
    assert str(artifact_root / "prompt/engine_research_rationale.md") in prompt
    assert "neutral `signal_packet.v2`" in prompt
    assert "directly against the frozen fixed-R target" in prompt
    assert "training/live parity" in prompt
    assert "packet-consumer" in prompt
    assert "Reject the engine hypothesis" in prompt

    assert "walk_forward_timestamp_labels.parquet" not in prompt
    assert "walk_forward_episodes.parquet" not in prompt
    assert "walk_forward_summary.json" not in prompt
    assert exact_opportunity_timestamp not in prompt
    assert exact_episode_id not in prompt
    assert '"label": "LONG"' not in prompt
    assert '"decision_ts"' not in prompt


def test_signal_engine_builder_skill_defines_outcome_first_discovery_rules() -> None:
    skill = (
        Path(__file__).resolve().parents[1] / "skills/signal-engine-builder/SKILL.md"
    ).read_text()

    assert "## Outcome-First Discovery" in skill
    assert "training-only" in skill
    assert "episode-level" in skill
    assert "directly against the frozen fixed-R target" in skill
    assert "no recurring causal mechanism" in skill
