from __future__ import annotations

from pathlib import Path
from typing import Any

from quant_terminal_worker.signal_discovery.workspace import read_frozen_target


_TRAINING_ARTIFACT_NAMES = (
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
    training_paths = tuple(atlas_root / name for name in _TRAINING_ARTIFACT_NAMES)
    missing = [str(path) for path in training_paths if not path.is_file()]
    if missing:
        raise ValueError(
            "engine builder prompt requires all training artifacts: " + ", ".join(missing)
        )

    prompt_path = root / "prompt" / "engine_builder_prompt.md"
    rationale_path = root / "prompt" / "engine_research_rationale.md"
    prompt = _render_engine_builder_prompt(
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
    )
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(prompt)
    return {
        "prompt_type": "signal_discovery_engine_builder",
        "session_id": str(target["session_id"]),
        "target_config_hash": str(target["config_hash"]),
        "prompt": prompt,
        "prompt_path": str(prompt_path),
        "rationale_path": str(rationale_path),
    }


def _render_engine_builder_prompt(
    *,
    session_id: str,
    target_config_hash: str,
    target_path: Path,
    training_paths: tuple[Path, ...],
    rationale_path: Path,
    registry_path: Path,
    engine_directory: Path,
    strategy_directory: Path,
) -> str:
    evidence_lines = [f"- `{path}`" for path in training_paths]
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
            "Do not inspect any walk-forward, validation, locked OOS, live-result, or future-outcome artifact.",
            "Do not reproduce timestamp-level outcome rows, exact opportunity timestamps, episode ids, "
            "or signal ids in source code, tests, rationale, or event rules.",
            "",
            "## Assignment",
            "",
            "Research whether the episode-level training evidence supports a recurring causal market "
            "mechanism that can be detected with information available at signal time. Compare opportunity "
            "episodes with their matched hard negatives, inspect stability across broad recurring buckets, "
            "and distinguish an economic mechanism from a coincidental feature threshold.",
            "",
            f"Write your evidence, competing hypotheses, stability checks, and decision to `{rationale_path}`. "
            "Reject the engine hypothesis when no coherent causal mechanism recurs; rejection is a valid "
            "result and must not be replaced by timestamp memorization, date-specific rules, or looser labels.",
            "",
            "If the hypothesis survives, implement exactly one candidate:",
            f"- Register it in `{registry_path}`.",
            f"- Put the engine adapter in `{engine_directory}`.",
            f"- Put the directional paired base strategy in `{strategy_directory}` and reference it from the registry entry.",
            "- Emit a neutral `signal_packet.v2`; direction belongs only in the paired strategy.",
            "- Use canonical Parquet and one shared packet builder for historical generation and live scanning.",
            "- Preserve the researched cadence/dedupe semantics across full generation, manual extension, and live extension.",
            "- Return the registered `signal_engine_id` and paired strategy path in the rationale.",
            "",
            "## Required Verification",
            "",
            "- Prove point-in-time safety for every trigger, feature, higher-timeframe row, and packet field.",
            "- Prove deterministic generation and training/live parity, including repeated-extension cadence state.",
            "- Run engine registry, neutral packet, paired-strategy, canonical wrapper, packet-consumer, and Stage 1 scorer contract tests.",
            "- Score event timestamp coverage and direction directly against the frozen fixed-R target using only the authorized training labels.",
            "- Report episode-level precision/recall, timestamp coverage, hard-negative false-positive rate, "
            "monthly stability, cadence, and duplicate/overlap behavior. Do not optimize only aggregate accuracy.",
            "- Do not change the fixed target, its barrier semantics, entry delay, holding horizon, costs, or split boundaries.",
            "- Do not claim walk-forward performance. The terminal will run held-out evaluation after the engine id is attached.",
            "",
        ]
    )
