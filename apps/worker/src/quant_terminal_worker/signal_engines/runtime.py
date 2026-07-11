from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import importlib
import json
from pathlib import Path
from typing import Any, Callable

from quant_terminal_sdk.engine_contracts import (
    ContractValidationError,
    LiveSignalScanResult,
    SignalEngineSpec,
    TrainingSignalGenerationResult,
)
from quant_terminal_sdk.market_data_reader import MarketDataReader


@dataclass(frozen=True, slots=True)
class EngineTrainingOutput:
    result: TrainingSignalGenerationResult
    packets: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class EngineTrainingContext:
    asset: str
    instrument: str
    signal_set: dict[str, Any]
    signal_set_key: str
    parameters: dict[str, Any]
    market_data_reader: MarketDataReader
    spec: SignalEngineSpec
    workspace_root: Path
    repository: Any
    start: datetime
    end: datetime
    raw_candle_end: datetime
    packet_sink: Callable[[list[dict[str, Any]]], None] | None = None
    packet_chunk_size: int = 500


@dataclass(frozen=True, slots=True)
class EngineLiveScanContext:
    asset: str
    instrument: str
    route: dict[str, Any]
    parameters: dict[str, Any]
    market_data_reader: MarketDataReader
    spec: SignalEngineSpec
    workspace_root: Path
    repository: Any


@dataclass(frozen=True, slots=True)
class ResolvedSignalEngine:
    spec: SignalEngineSpec
    generate_training_signals: Callable[[EngineTrainingContext], EngineTrainingOutput]
    scan_live_signal: Callable[[EngineLiveScanContext], LiveSignalScanResult]


def apply_fixed_engine_parameters(
    spec: SignalEngineSpec,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(parameters)
    schema = spec.configuration_schema if isinstance(spec.configuration_schema, dict) else {}
    fixed = schema.get("fixed_parameters")
    if isinstance(fixed, dict):
        merged.update(fixed)
    return merged


def resolve_signal_engine(
    signal_engine_id: str,
    *,
    version: str | None = None,
    repository: Any,
    workspace_root: Path,
) -> ResolvedSignalEngine:
    spec = _resolve_spec(
        signal_engine_id,
        version=version,
        repository=repository,
        workspace_root=workspace_root,
    )
    if spec.signal_engine_id == "vegas_ema":
        from quant_terminal_worker.signal_engines.vegas_ema import (
            generate_training_signals,
            scan_live_signal,
        )

        return ResolvedSignalEngine(
            spec=spec,
            generate_training_signals=generate_training_signals,
            scan_live_signal=scan_live_signal,
        )
    return ResolvedSignalEngine(
        spec=spec,
        generate_training_signals=_load_training_entrypoint(spec.runtime_entrypoint),
        scan_live_signal=_load_live_entrypoint(spec.live_scanner_entrypoint),
    )


def _resolve_spec(
    signal_engine_id: str,
    *,
    version: str | None,
    repository: Any,
    workspace_root: Path,
) -> SignalEngineSpec:
    for engine in _repository_engines(repository):
        if engine.get("signal_engine_id") != signal_engine_id:
            continue
        if version is not None and engine.get("version") != version:
            continue
        return SignalEngineSpec.from_mapping(_with_spec_defaults(engine))
    registry_path = workspace_root / "artifacts" / "signal_engine" / "engine_registry.json"
    if registry_path.is_file():
        registry = json.loads(registry_path.read_text())
        entry = registry.get(signal_engine_id) if isinstance(registry, dict) else None
        if isinstance(entry, dict):
            if version is not None and entry.get("version") not in (None, version):
                raise ContractValidationError(
                    f"signal engine {signal_engine_id} version {version} not found"
                )
            return SignalEngineSpec.from_mapping(_with_spec_defaults(entry))
    raise ContractValidationError(f"signal engine not found: {signal_engine_id}")


def _repository_engines(repository: Any) -> list[dict[str, Any]]:
    if not hasattr(repository, "list_signal_engines"):
        return []
    return list(repository.list_signal_engines())


def _with_spec_defaults(value: dict[str, Any]) -> dict[str, Any]:
    configuration_schema = value.get("configuration_schema") if isinstance(value.get("configuration_schema"), dict) else {}
    code_ref = value.get("code_ref") if isinstance(value.get("code_ref"), dict) else {}
    default_parameters = code_ref.get("default_parameters")
    if isinstance(default_parameters, dict) and "default_parameters" not in configuration_schema:
        configuration_schema = {**configuration_schema, "default_parameters": default_parameters}
    return {
        **value,
        "output_envelope_version": value.get("output_envelope_version") or "signal_packet.v2",
        "configuration_schema": configuration_schema,
    }


def _load_training_entrypoint(entrypoint: str) -> Callable[[EngineTrainingContext], EngineTrainingOutput]:
    function = _load_callable(entrypoint)

    def generate(context: EngineTrainingContext) -> EngineTrainingOutput:
        raw = function(context)
        return _coerce_training_output(raw)

    return generate


def _load_live_entrypoint(entrypoint: str) -> Callable[[EngineLiveScanContext], LiveSignalScanResult]:
    function = _load_callable(entrypoint)

    def scan(context: EngineLiveScanContext) -> LiveSignalScanResult:
        raw = function(context)
        if isinstance(raw, LiveSignalScanResult):
            return raw
        if isinstance(raw, dict):
            return LiveSignalScanResult(**raw)
        raise ContractValidationError("live scanner must return LiveSignalScanResult or mapping")

    return scan


def _load_callable(entrypoint: str) -> Callable[..., Any]:
    if ":" not in entrypoint:
        raise ContractValidationError(f"entrypoint must be module:function: {entrypoint}")
    module_name, function_name = entrypoint.split(":", 1)
    module = importlib.import_module(module_name)
    function = getattr(module, function_name, None)
    if not callable(function):
        raise ContractValidationError(f"entrypoint is not callable: {entrypoint}")
    return function


def _coerce_training_output(raw: Any) -> EngineTrainingOutput:
    if isinstance(raw, EngineTrainingOutput):
        return raw
    if isinstance(raw, dict):
        result = raw.get("result")
        packets = list(raw.get("packets") or [])
        if isinstance(result, TrainingSignalGenerationResult):
            return EngineTrainingOutput(result=result, packets=packets)
        if isinstance(result, dict):
            return EngineTrainingOutput(result=TrainingSignalGenerationResult(**result), packets=packets)
    raise ContractValidationError("training generator must return EngineTrainingOutput or mapping")
