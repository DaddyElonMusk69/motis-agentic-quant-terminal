from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from quant_terminal_worker.signal_discovery.supervised_training_data import (
    MODULE_IMPORT,
    prepare_supervised_training_data,
    select_training_datasets,
)
from quant_terminal_worker.signal_discovery.workspace import read_frozen_target


_OUTCOME_FIRST_TRAINING_ARTIFACT_NAMES = (
    "training_timestamp_labels.parquet",
    "training_episodes.parquet",
    "training_features.parquet",
    "training_hard_negatives.parquet",
)


def generate_engine_builder_prompt(
    *,
    workspace_root: str | Path,
    artifact_root: str | Path,
) -> dict[str, Any]:
    workspace = Path(workspace_root).resolve()
    root = Path(artifact_root).resolve()
    target = read_frozen_target(artifact_root=root)
    atlas_root = root / "atlas"
    prompt_path = root / "prompt" / "engine_builder_prompt.md"
    evidence_manifest_path = root / "evidence" / "evidence_manifest.json"
    evidence_manifest = (
        json.loads(evidence_manifest_path.read_text()) if evidence_manifest_path.is_file() else None
    )
    supervised = _supports_supervised_preparation(evidence_manifest)
    prepared_manifest_path: Path | None = None
    if supervised:
        training_paths = (
            atlas_root / "training_timestamp_labels.parquet",
            atlas_root / "training_features.parquet",
        )
        rationale_path = root / "prompt" / "supervised_training_rationale.md"
        _require_training_paths(training_paths)
        prepared = prepare_supervised_training_data(
            workspace_root=workspace,
            artifact_root=root,
        )
        prepared_manifest_path = Path(prepared["manifest_path"])
        training_paths = (*training_paths, prepared_manifest_path)
        prompt = _render_supervised_engine_builder_prompt(
            session_id=str(target["session_id"]),
            target_config_hash=str(target["config_hash"]),
            target_path=root / "target" / "frozen_target.json",
            training_paths=training_paths,
            rationale_path=rationale_path,
            evidence_manifest_path=evidence_manifest_path,
            evidence_manifest=evidence_manifest,
            prepared_manifest_path=prepared_manifest_path,
            prepared_manifest=prepared["manifest"],
        )
        workflow = "supervised_timestamp_training"
    else:
        bracket_contract = target.get("bracket_policy") or {}
        training_paths = (
            (
                root / str(bracket_contract["training_brackets_path"]),
                atlas_root / "training_features.parquet",
                root / str(bracket_contract["training_hard_negatives_path"]),
            )
            if bracket_contract
            else tuple(
                atlas_root / name for name in _OUTCOME_FIRST_TRAINING_ARTIFACT_NAMES
            )
        )
        rationale_path = root / "prompt" / "engine_research_rationale.md"
        _require_training_paths(training_paths)
        prompt = _render_outcome_first_engine_builder_prompt(
            session_id=str(target["session_id"]),
            target_config_hash=str(target["config_hash"]),
            target_path=root / "target" / "frozen_target.json",
            training_paths=training_paths,
            rationale_path=rationale_path,
            registry_path=workspace / "artifacts" / "signal_engine" / "engine_registry.json",
            engine_directory=workspace
            / "apps"
            / "worker"
            / "src"
            / "quant_terminal_worker"
            / "signal_engines",
            strategy_directory=workspace
            / "packages"
            / "strategy_modules"
            / "src"
            / "quant_terminal_strategies",
            evidence_manifest_path=(
                evidence_manifest_path if evidence_manifest is not None else None
            ),
            evidence_manifest=evidence_manifest,
        )
        workflow = "outcome_first_discovery"
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(prompt)
    result = {
        "prompt_type": "signal_discovery_engine_builder",
        "workflow": workflow,
        "session_id": str(target["session_id"]),
        "target_config_hash": str(target["config_hash"]),
        "prompt": prompt,
        "prompt_path": str(prompt_path),
        "rationale_path": str(rationale_path),
    }
    if prepared_manifest_path is not None:
        result["prepared_training_manifest_path"] = str(prepared_manifest_path)
    return result


def _supports_supervised_preparation(evidence_manifest: dict[str, Any] | None) -> bool:
    if evidence_manifest is None:
        return False
    try:
        select_training_datasets(evidence_manifest)
    except ValueError:
        return False
    return True


def _require_training_paths(paths: tuple[Path, ...]) -> None:
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise ValueError(
            "engine builder prompt requires all training artifacts: " + ", ".join(missing)
        )


def _render_supervised_engine_builder_prompt(
    *,
    session_id: str,
    target_config_hash: str,
    target_path: Path,
    training_paths: tuple[Path, ...],
    rationale_path: Path,
    evidence_manifest_path: Path | None = None,
    evidence_manifest: dict[str, Any] | None = None,
    prepared_manifest_path: Path,
    prepared_manifest: dict[str, Any],
) -> str:
    evidence_lines = [f"- `{path}`" for path in training_paths]
    market_evidence_lines: list[str] = []
    if evidence_manifest_path is not None and evidence_manifest is not None:
        market_evidence_lines = [
            "",
            "## Authorized Canonical Market Evidence",
            "",
            f"Evidence manifest: `{evidence_manifest_path}`",
            f"The row-level authorization cutoff is `research_end` = "
            f"`{evidence_manifest['authorized_end']}`. Apply this cutoff when reading every source, "
            "including shards that also contain later rows.",
            "This manifest is the complete authorized source inventory and provenance record. The repo-level "
            "training-data module has already selected and transformed the canonical inputs listed in the "
            "prepared manifest below.",
            "Use the prepared manifest as the frozen model-input schema. Do not bypass it with session-local "
            "feature engineering, directly add another registered dataset, or rebuild its timelines in the "
            "training script. A channel change belongs in the repo-level module and requires a new prepared "
            "feature schema and retraining.",
            "Sources listed here but absent from the prepared manifest are inventory evidence only for this "
            "run; they are not additional model inputs.",
            "",
            "Authorized registered sources:",
            *[
                "- `{dataset_id}` | `{data_type}` | `{timeframe}` | `{data_origin}` | `{storage_uri}`".format(
                    dataset_id=row.get("dataset_id"),
                    data_type=row.get("data_type"),
                    timeframe=row.get("timeframe") or "none",
                    data_origin=row.get("data_origin"),
                    storage_uri=row.get("storage_uri"),
                )
                for row in evidence_manifest.get("included_datasets", ())
            ],
        ]
    branch_lines = [
        "- `{name}`: `{channels}` raw channels plus masks x `{steps}` ordered steps "
        "({days} days)".format(
            name=name,
            channels=len(entry["channel_names"]),
            steps=entry["spec"]["steps"],
            days=entry["spec"]["lookback_days"],
        )
        for name, entry in prepared_manifest["branches"].items()
    ]
    return "\n".join(
        [
            "# Supervised Timestamp Signal Engine Builder Prompt",
            "",
            "Use `$signal-engine-builder` and follow the supervised timestamp-training contract. "
            "Ignore episode-first discovery and engine-registration instructions that conflict with this handoff.",
            "",
            f"Session: `{session_id}`",
            f"Frozen target config hash: `{target_config_hash}`",
            "",
            "## Authorized Training Evidence",
            "",
            f"Frozen target contract: `{target_path}`",
            *evidence_lines,
            "",
            "These are the only outcome-label artifacts you may inspect. They are training-only. "
            "Do not inspect walk-forward, locked OOS, live-result, or future-outcome artifacts. Internal "
            "chronological validation must use only the research period.",
            "Do not reproduce exact labeled timestamps in source code, tests, reports, or model rules.",
            *market_evidence_lines,
            "",
            "## Prepared Training Data",
            "",
            f"Prepared manifest: `{prepared_manifest_path}`",
            f"Load with `{MODULE_IMPORT}` or `load_prepared_supervised_training_data(...)` from the same module.",
            f"Prepared label rows: `{prepared_manifest['labels']['eligible_rows']}`; positive prevalence: "
            f"`{prepared_manifest['labels']['positive_prevalence']}`.",
            "The prepared timelines are unscaled. Fit median/IQR normalization separately on unique feature "
            "rows inside each chronological training fold, then apply that frozen fold state to validation. "
            "Never fit one global scaler before splitting.",
            "Prepared branches:",
            *branch_lines,
            "",
            "## Training Objective",
            "",
            "Train a causal sequence model that predicts whether each exact eligible 5m decision timestamp "
            "satisfies the frozen neutral fixed-R target. Use raw labels directly: `LONG` and `SHORT` map to "
            "neutral target `1`; `NEUTRAL` maps to `0`; `AMBIGUOUS` is excluded. Do not construct, merge, filter, "
            "weight, or score episodes or brackets. Every included timestamp has base weight `1`.",
            "Use the dense oldest-to-newest branch tensors. Any temporal compression must be learned inside the "
            "model or justified with an information-preservation test; do not recreate uniform 32-bin summaries.",
            "",
            "## Required Training Process",
            "",
            "- Prove the model can overfit a small chronological training subset before a full run.",
            "- Run a shuffled-label control; it must return to the prevalence baseline.",
            "- Use expanding chronological folds with target-horizon purge and fold-fitted preprocessing.",
            "- Use unit timestamp weights and include every ordinary negative.",
            "- Select epochs by internal validation and retain the best checkpoint; do not use a fixed shallow pass count without evidence.",
            "- Report raw timestamp precision, recall, F1, PR-AUC, ROC-AUC diagnostic, calibration, full threshold frontier, monthly stability, and fold/seed results.",
            "- Compare against natural prevalence and causal tabular baselines on the same timestamps.",
            "- Do not change the fixed target, its barrier semantics, entry delay, holding horizon, costs, or split boundaries.",
            "- Do not inspect or score walk-forward data.",
            "",
            f"Write the data audit, architecture, controls, fold results, accepted/rejected decision, and artifact hashes to `{rationale_path}`.",
            "Do not register an engine, build a live scanner, advance Stage 1, or run walk-forward in this task. "
            "A rejected model is a valid result.",
            "",
        ]
    )


def _render_outcome_first_engine_builder_prompt(
    *,
    session_id: str,
    target_config_hash: str,
    target_path: Path,
    training_paths: tuple[Path, ...],
    rationale_path: Path,
    registry_path: Path,
    engine_directory: Path,
    strategy_directory: Path,
    evidence_manifest_path: Path | None = None,
    evidence_manifest: dict[str, Any] | None = None,
) -> str:
    evidence_lines = [f"- `{path}`" for path in training_paths]
    market_evidence_lines: list[str] = []
    if evidence_manifest_path is not None and evidence_manifest is not None:
        market_evidence_lines = [
            "",
            "## Authorized Canonical Market Evidence",
            "",
            f"Evidence manifest: `{evidence_manifest_path}`",
            f"The row-level authorization cutoff is `research_end` = "
            f"`{evidence_manifest['authorized_end']}`. Apply this cutoff when reading every source, "
            "including shards that also contain later rows.",
            "The manifest's included datasets are all fair game for evaluation. You may perform "
            "arbitrary causal resampling and derived-feature research, but verify availability semantics "
            "before use.",
            "`training_features.parquet` is a convenience baseline, not the feature-search boundary.",
            "",
            "Authorized registered sources:",
            *[
                "- `{dataset_id}` | `{data_type}` | `{timeframe}` | `{data_origin}` | `{storage_uri}`".format(
                    dataset_id=row.get("dataset_id"),
                    data_type=row.get("data_type"),
                    timeframe=row.get("timeframe") or "none",
                    data_origin=row.get("data_origin"),
                    storage_uri=row.get("storage_uri"),
                )
                for row in evidence_manifest.get("included_datasets", ())
            ],
        ]
    return "\n".join(
        [
            "# Outcome-First Signal Engine Builder Prompt",
            "",
            "Use `$signal-engine-builder` and follow its Outcome-First Discovery workflow.",
            "",
            f"Session: `{session_id}`",
            f"Frozen target config hash: `{target_config_hash}`",
            "",
            "## Authorized Discovery Evidence",
            "",
            f"Frozen target contract: `{target_path}`",
            *evidence_lines,
            "",
            "These are the only discovery evidence artifacts you may inspect. They are training-only. "
            "Do not inspect walk-forward, validation, locked OOS, live-result, or future-outcome artifacts.",
            "Do not reproduce timestamp-level outcome rows, exact opportunity timestamps, episode ids, "
            "or signal ids in source code, tests, rationale, or event rules.",
            *market_evidence_lines,
            "",
            "## Assignment",
            "",
            "Research whether the episode-level evidence supports a recurring causal market mechanism. "
            "Compare opportunity episodes with matched hard negatives and reject calendar-specific or "
            "unsupported rules.",
            "Treat approved brackets as opportunity regions. Evaluate the final globally deduped stream "
            "with deterministic emitted episodes and one-to-one bracket matching.",
            "Use only evaluator-declared temporal tolerance. If none is declared, the canonical signal "
            "availability timestamp must fall inside the bracket.",
            "A bounded conditional tree with causal OR-composed leaves is allowed, but every retained leaf "
            "must add unique post-dedupe coverage and survive chronological and threshold perturbation checks.",
            "",
            f"Write evidence, competing hypotheses, stability checks, and the decision to `{rationale_path}`. "
            "Rejection is valid and must not be replaced by timestamp memorization or looser labels.",
            "Document every dataset, column, lookback, transformation, availability proof, rejected hypothesis, "
            "and final production dependency.",
            "",
            "If the hypothesis survives, implement exactly one candidate:",
            f"- Register it in `{registry_path}`.",
            f"- Put the engine adapter in `{engine_directory}`.",
            f"- Put the paired base strategy in `{strategy_directory}` and reference it from the registry entry.",
            "- Emit a neutral `signal_packet.v2`; direction belongs only in the paired strategy.",
            "- Use canonical Parquet and one shared packet builder for historical and live scanning.",
            "- Preserve researched cadence and dedupe semantics across generation and extension.",
            "",
            "## Required Verification",
            "",
            "- Prove point-in-time safety for triggers, features, resampling, joins, and packet fields.",
            "- Prove deterministic generation, repeated-extension cadence, and training/live parity.",
            "- Run registry, packet, paired-strategy, canonical-wrapper, consumer, and scorer contract tests.",
            "- Report one-to-one episode precision, bracket-count coverage, unmatched episodes, hard-negative "
            "rate, raw timestamp diagnostics, monthly stability, cadence, and drought.",
            "- Do not change the target, barrier semantics, entry delay, horizon, costs, or split boundaries.",
            "- Do not claim walk-forward performance; terminal-owned evaluation remains sealed.",
            "",
        ]
    )
