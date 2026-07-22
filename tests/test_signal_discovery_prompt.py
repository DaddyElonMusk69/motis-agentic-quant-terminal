from __future__ import annotations

from pathlib import Path
import json

import pyarrow as pa
import pyarrow.parquet as pq

from quant_terminal_worker.signal_discovery import prompt as prompt_module
from quant_terminal_worker.signal_discovery.prompt import generate_engine_builder_prompt
from quant_terminal_worker.signal_discovery.workspace import freeze_target_contract


def test_engine_builder_prompt_is_deterministic_training_only_and_actionable(
    tmp_path: Path,
    monkeypatch,
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
    evidence_root = artifact_root / "evidence"
    evidence_root.mkdir()
    (evidence_root / "evidence_manifest.json").write_text(
        json.dumps(
            {
                "authorized_end": "2026-03-31T23:55:00Z",
                "included_datasets": [],
            }
        )
        + "\n"
    )

    def prepare_stub(*, workspace_root, artifact_root):
        del workspace_root
        manifest_path = Path(artifact_root) / "training/supervised_input/manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest = {
            "labels": {"eligible_rows": 1, "positive_prevalence": 1.0},
            "branches": {
                "5m_micro": {
                    "channel_names": ["price_log_return"],
                    "spec": {"steps": 2016, "lookback_days": 7.0},
                }
            },
        }
        manifest_path.write_text(json.dumps(manifest) + "\n")
        return {"manifest_path": str(manifest_path), "manifest": manifest}

    monkeypatch.setattr(prompt_module, "prepare_supervised_training_data", prepare_stub)
    monkeypatch.setattr(
        prompt_module,
        "_supports_supervised_preparation",
        lambda evidence_manifest: evidence_manifest is not None,
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
    assert first["workflow"] == "supervised_timestamp_training"
    assert "$signal-engine-builder" in prompt
    assert str(atlas_root / "training_timestamp_labels.parquet") in prompt
    assert str(atlas_root / "training_features.parquet") in prompt
    assert str(artifact_root / "target/frozen_target.json") in prompt
    assert str(artifact_root / "training/supervised_input/manifest.json") in prompt
    assert str(artifact_root / "prompt/supervised_training_rationale.md") in prompt
    assert "raw labels directly" in prompt
    assert "LONG` and `SHORT" in prompt
    assert "Do not construct, merge, filter" in prompt
    assert "uniform 32-bin summaries" in prompt
    assert "overfit a small chronological training subset" in prompt
    assert "shuffled-label control" in prompt
    assert "fold-fitted preprocessing" in prompt
    assert "Use the prepared manifest as the frozen model-input schema" in prompt
    assert "Do not bypass it with session-local feature engineering" in prompt
    assert "Do not register an engine" in prompt
    assert "advance Stage 1" in prompt
    assert "Do not inspect or score walk-forward data" in prompt

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
    assert "signal_discovery_evidence_manifest.v1" in skill
    assert "research_end" in skill
    assert "arbitrary causal" in skill
    assert "convenience baseline" in skill
    assert "dataset ids" in skill
    assert "production dependencies" in skill
    assert "Treat approved brackets as opportunity regions" in skill
    assert "episode precision" in skill
    assert "bracket-count coverage" in skill
    assert "final globally deduped signal stream" in skill
    assert "conditional tree" in skill
    assert "OR-composed leaves" in skill
    assert "minimum independent episode support" in skill
    assert "internal chronological blocks" in skill
    assert "zero unless the target contract explicitly defines one" in skill
    assert "## Supervised Timestamp Training" in skill
    assert "motis_supervised_training_input.v1" in skill
    assert "map `LONG` and `SHORT` to neutral target `1`" in skill
    assert "Do not create session-local feature engineering" in skill
    assert "shuffled-label control" in skill
    assert "raw timestamp scores before dedupe" in skill
