from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType
from typing import Any

from quant_terminal_worker.execution.decision_cadence import build_decision_cadence_contract


def load_execution_bundle(bundle: dict[str, Any], *, workspace_root: Path) -> dict[str, Any]:
    bundle_root = Path(bundle["bundle_uri"])
    if not bundle_root.is_absolute():
        bundle_root = workspace_root / bundle_root
    strategy_path = Path(bundle["strategy_module_ref"])
    if not strategy_path.is_absolute():
        strategy_path = workspace_root / strategy_path
    setup_path = bundle_root / "execution_setup.json"
    execution_setup = bundle.get("execution_setup") or {}
    if setup_path.is_file():
        execution_setup = json.loads(setup_path.read_text())
    if isinstance(execution_setup, dict) and not isinstance(execution_setup.get("sizing"), dict):
        sizing = _stage4_sizing_from_bundle(bundle=bundle, workspace_root=workspace_root)
        if sizing:
            execution_setup = {**execution_setup, "sizing": sizing}
    if isinstance(execution_setup, dict) and not isinstance(execution_setup.get("decision_cadence"), dict):
        cadence = _stage4_decision_cadence_from_bundle(bundle=bundle, workspace_root=workspace_root)
        if cadence:
            execution_setup = {**execution_setup, "decision_cadence": cadence}
    return {
        "bundle": bundle,
        "bundle_root": bundle_root,
        "strategy_path": strategy_path,
        "strategy_module": load_strategy_module(strategy_path),
        "execution_setup": execution_setup,
    }


def load_strategy_module(strategy_path: Path) -> ModuleType:
    if not strategy_path.is_file():
        raise FileNotFoundError(f"strategy module not found: {strategy_path}")
    module_name = f"motis_execution_strategy_{abs(hash(str(strategy_path)))}"
    spec = importlib.util.spec_from_file_location(module_name, strategy_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load strategy module: {strategy_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _stage4_sizing_from_bundle(*, bundle: dict[str, Any], workspace_root: Path) -> dict[str, Any]:
    payload = _stage4_payload_from_bundle(bundle=bundle, workspace_root=workspace_root)
    if not payload:
        return {}
    inputs = payload.get("simulation_inputs") if isinstance(payload.get("simulation_inputs"), dict) else {}
    margin = _positive_number(inputs.get("margin_allocation_pct"))
    leverage = _positive_number(inputs.get("leverage"))
    if margin is None or leverage is None:
        return {}
    return {
        "source": "stage4_realized_expectancy",
        "initial_capital_usdt": inputs.get("initial_capital_usdt"),
        "margin_allocation_pct": margin,
        "leverage": leverage,
    }


def _stage4_decision_cadence_from_bundle(*, bundle: dict[str, Any], workspace_root: Path) -> dict[str, Any]:
    payload = _stage4_payload_from_bundle(bundle=bundle, workspace_root=workspace_root)
    if not payload:
        return {}
    cadence = payload.get("decision_cadence")
    if isinstance(cadence, dict):
        return dict(cadence)
    windows = payload.get("slice_windows") if isinstance(payload.get("slice_windows"), list) else []
    ends = [window.get("end") for window in windows if isinstance(window, dict) and window.get("end")]
    if not ends:
        return {}
    return build_decision_cadence_contract(
        cursor_seed_timestamp=max(ends),
        source_stage4_run_id=payload.get("run_id"),
        source_stage4_candidate_id=payload.get("best_candidate_id"),
    )


def _stage4_payload_from_bundle(*, bundle: dict[str, Any], workspace_root: Path) -> dict[str, Any]:
    path_value = bundle.get("source_stage4_result_path")
    if not isinstance(path_value, str) or not path_value:
        evidence_refs = bundle.get("evidence_refs") if isinstance(bundle.get("evidence_refs"), dict) else {}
        path_value = evidence_refs.get("stage4_realized_expectancy")
    if not isinstance(path_value, str) or not path_value:
        return {}
    stage4_path = Path(path_value)
    if not stage4_path.is_absolute():
        stage4_path = workspace_root / stage4_path
    if not stage4_path.is_file():
        return {}
    payload = json.loads(stage4_path.read_text())
    return payload if isinstance(payload, dict) else {}


def _positive_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None
