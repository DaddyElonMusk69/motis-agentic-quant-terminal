from __future__ import annotations

from dataclasses import dataclass, field
import importlib.util
import json
from pathlib import Path
from typing import Any, Literal


SUPPORTED_REQUIRED_DATA_TYPES = {
    "candles",
    "feature_base_candle",
    "feature_volatility_range",
    "feature_volume",
    "feature_ema_vegas_structure",
    "feature_bollinger",
    "feature_regime_momentum",
}
SUPPORTED_REQUIRED_DATA_ORIGINS = {"raw", "derived"}
SUPPORTED_PACKET_SCHEMA = "signal_packet.v2"
FORBIDDEN_SIGNAL_PACKET_FIELDS = {
    "action",
    "confidence",
    "direction",
    "entry",
    "entry_price",
    "leverage",
    "margin",
    "notional_usd",
    "order_type",
    "position_size",
    "score",
    "side",
    "size",
    "sl",
    "sl_pct",
    "stop_loss",
    "take_profit",
    "tp",
    "tp_pct",
    "trade_action",
}
ENTRY_ACTIONS = {"ENTER", "ENTER_LONG", "ENTER_SHORT"}
STRATEGY_ACTIONS = ENTRY_ACTIONS | {"SKIP", "BLOCKED"}
POSITION_MANAGEMENT_ACTIONS = {"HOLD", "EXIT", "REDUCE", "PYRAMID", "UPDATE_PROTECTION", "BLOCKED"}


class ContractValidationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class SignalEngineSpec:
    signal_engine_id: str
    version: str
    required_data: list[dict[str, Any]]
    output_envelope_version: str
    runtime_entrypoint: str
    live_scanner_entrypoint: str
    name: str = ""
    description: str = ""
    code_ref: dict[str, Any] = field(default_factory=dict)
    configuration_schema: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "SignalEngineSpec":
        _require_nonempty(value, "signal_engine_id")
        is_legacy_entry = bool(value.get("replay_generator_path") or value.get("live_scanner_path"))
        version = str(value.get("version") or value.get("signal_engine_version") or ("legacy" if is_legacy_entry else "")).strip()
        if not version:
            raise ContractValidationError("version is required")
        runtime_entrypoint = str(
            value.get("runtime_entrypoint") or value.get("training_generator_entrypoint") or value.get("replay_generator_path") or ""
        ).strip()
        live_scanner_entrypoint = str(value.get("live_scanner_entrypoint") or value.get("live_scanner_path") or "").strip()
        if not runtime_entrypoint:
            raise ContractValidationError("runtime_entrypoint is required")
        if not live_scanner_entrypoint:
            raise ContractValidationError("live_scanner_entrypoint is required")
        output_envelope_version = str(value.get("output_envelope_version") or SUPPORTED_PACKET_SCHEMA).strip()
        if output_envelope_version != SUPPORTED_PACKET_SCHEMA:
            raise ContractValidationError(f"output_envelope_version must be {SUPPORTED_PACKET_SCHEMA}")
        required_data = list(value.get("required_data") or [])
        _validate_required_data(required_data)
        return cls(
            signal_engine_id=str(value["signal_engine_id"]).strip(),
            version=version,
            required_data=required_data,
            output_envelope_version=output_envelope_version,
            runtime_entrypoint=runtime_entrypoint,
            live_scanner_entrypoint=live_scanner_entrypoint,
            name=str(value.get("name") or value.get("signal_engine_id") or "").strip(),
            description=str(value.get("description") or ""),
            code_ref=value.get("code_ref") if isinstance(value.get("code_ref"), dict) else {},
            configuration_schema=value.get("configuration_schema") if isinstance(value.get("configuration_schema"), dict) else {},
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "signal_engine_id": self.signal_engine_id,
            "name": self.name,
            "version": self.version,
            "runtime_entrypoint": self.runtime_entrypoint,
            "live_scanner_entrypoint": self.live_scanner_entrypoint,
            "description": self.description,
            "code_ref": self.code_ref,
            "required_data": self.required_data,
            "output_envelope_version": self.output_envelope_version,
            "configuration_schema": self.configuration_schema,
        }


@dataclass(frozen=True, slots=True)
class SignalPacket:
    schema_version: str
    asset: str
    timestamp: str
    evidence: dict[str, Any]
    instrument: str | None = None
    active_timeframes: list[str] = field(default_factory=list)

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "SignalPacket":
        validate_signal_packet(value)
        return cls(
            schema_version=str(value["schema_version"]),
            asset=str(value["asset"]),
            timestamp=str(value["timestamp"]),
            instrument=str(value["instrument"]) if value.get("instrument") not in (None, "") else None,
            active_timeframes=[str(item) for item in value.get("active_timeframes") or []],
            evidence=value.get("evidence") if isinstance(value.get("evidence"), dict) else _derived_evidence(value),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "asset": self.asset,
            "instrument": self.instrument,
            "timestamp": self.timestamp,
            "active_timeframes": self.active_timeframes,
            "evidence": self.evidence,
        }


@dataclass(frozen=True, slots=True)
class TrainingSignalGenerationResult:
    status: Literal["appended", "noop", "blocked"]
    generated_packet_count: int
    appended_packet_count: int
    raw_candle_end_ts: str | None
    scan_coverage_end_ts: str | None
    packet_refs: list[str] = field(default_factory=list)
    final_signal_end_ts: str | None = None
    previous_signal_end_ts: str | None = None


@dataclass(frozen=True, slots=True)
class LiveSignalScanResult:
    status: Literal["fresh_signal", "no_fresh_signal", "blocked"]
    source: Literal["live_parquet_snapshot"]
    signal: SignalPacket | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.status == "fresh_signal" and self.signal is None:
            raise ContractValidationError("fresh live scan results require signal")
        if self.status != "fresh_signal" and self.signal is not None:
            raise ContractValidationError("non-fresh live scan results must not include signal")


def validate_engine_registry_entry(value: dict[str, Any]) -> list[str]:
    SignalEngineSpec.from_mapping(value)
    return []


def validate_signal_engine_spec(path_or_engine_id: str | Path) -> list[str]:
    value = _load_signal_engine_spec_mapping(path_or_engine_id)
    return validate_engine_registry_entry(value)


def validate_signal_packet(packet: dict[str, Any]) -> list[str]:
    _require_nonempty(packet, "schema_version")
    _require_nonempty(packet, "asset")
    _require_nonempty(packet, "timestamp")
    if packet["schema_version"] != SUPPORTED_PACKET_SCHEMA:
        raise ContractValidationError(f"schema_version must be {SUPPORTED_PACKET_SCHEMA}")
    _reject_forbidden_packet_fields(packet, path="")
    return []


def validate_strategy_module(strategy_path: str | Path) -> list[str]:
    module = _load_module(Path(strategy_path))
    decide = getattr(module, "decide", None)
    if not callable(decide):
        raise ContractValidationError("strategy module must expose callable decide(context)")
    decision = decide(_sample_decide_context())
    if not isinstance(decision, dict):
        raise ContractValidationError("decide(context) must return a dict")
    _validate_strategy_decision_dict(decision)
    manage_position = getattr(module, "manage_position", None)
    if manage_position is not None:
        if not callable(manage_position):
            raise ContractValidationError("manage_position must be callable when defined")
        management_decision = manage_position(_sample_position_context())
        if not isinstance(management_decision, dict):
            raise ContractValidationError("manage_position(context) must return a dict")
        _validate_position_management_decision_dict(management_decision)
    return []


def validate_execution_bundle_contract(bundle: dict[str, Any]) -> list[str]:
    setup_root = bundle.get("execution_setup") if isinstance(bundle.get("execution_setup"), dict) else bundle
    setup = setup_root.get("setup") if isinstance(setup_root.get("setup"), dict) else setup_root
    if (
        setup_root.get("forward_hours") in (None, "")
        and setup.get("forward_hours") in (None, "")
        and setup.get("max_hold_hours") in (None, "")
    ):
        raise ContractValidationError("execution setup missing forward_hours")
    if (
        setup_root.get("hard_exit_after_hours") in (None, "")
        and setup.get("hard_exit_after_hours") in (None, "")
        and setup.get("max_hold_hours") in (None, "")
    ):
        raise ContractValidationError("execution setup missing hard_exit_after_hours")
    if setup.get("policy_mode") == "side_specific":
        _validate_side_specific_execution_setup(setup)
    else:
        _validate_exit_policy_fields(setup, label="execution setup")
    pyramid = setup.get("pyramid")
    if pyramid is not None:
        if not isinstance(pyramid, dict):
            raise ContractValidationError("execution setup pyramid must be an object")
        if pyramid.get("max_legs") in (None, ""):
            raise ContractValidationError("execution setup pyramid missing max_legs")
        if int(pyramid["max_legs"]) > 1 and pyramid.get("step_pct") in (None, ""):
            raise ContractValidationError("execution setup pyramid missing step_pct")
    return []


def validate_execution_bundle(path_or_bundle_id: str | Path) -> list[str]:
    return validate_execution_bundle_contract(_load_execution_bundle_mapping(path_or_bundle_id))


def _validate_side_specific_execution_setup(setup: dict[str, Any]) -> None:
    side_policies = setup.get("side_policies")
    if not isinstance(side_policies, dict):
        raise ContractValidationError("side-specific execution setup missing side_policies")
    for side in ("LONG", "SHORT"):
        policy = side_policies.get(side)
        if not isinstance(policy, dict):
            raise ContractValidationError(f"{side} execution setup missing side policy")
        _validate_exit_policy_fields(policy, label=f"{side} execution setup")


def _validate_exit_policy_fields(setup: dict[str, Any], *, label: str) -> None:
    final_tp = _first_present(setup, "final_tp_pct", "tp_pct", "lock_profit_pct")
    initial_sl = _first_present(setup, "initial_sl_pct", "sl_pct")
    if final_tp in (None, ""):
        raise ContractValidationError(f"{label} missing final_tp_pct")
    if initial_sl in (None, ""):
        raise ContractValidationError(f"{label} missing initial_sl_pct")
    protection_enabled = _truthy(setup.get("protection_enabled"))
    if protection_enabled:
        if setup.get("protect_trigger_pct") in (None, ""):
            raise ContractValidationError(f"protected {label} missing protect_trigger_pct")
        if setup.get("trail_sl_pct") in (None, ""):
            raise ContractValidationError(f"protected {label} missing trail_sl_pct")
        if _numeric(setup.get("protect_trigger_pct")) <= 0:
            raise ContractValidationError(f"protected {label} requires positive protect_trigger_pct")
        if _numeric(setup.get("trail_sl_pct")) <= 0:
            raise ContractValidationError(f"protected {label} requires positive trail_sl_pct")


def _validate_required_data(required_data: list[dict[str, Any]]) -> None:
    for requirement in required_data:
        data_type = str(requirement.get("data_type") or "").strip()
        origin = str(requirement.get("origin") or requirement.get("data_origin") or "").strip()
        if data_type not in SUPPORTED_REQUIRED_DATA_TYPES:
            raise ContractValidationError(f"unsupported required data type: {data_type}")
        if origin not in SUPPORTED_REQUIRED_DATA_ORIGINS:
            raise ContractValidationError(f"unsupported required data origin: {origin}")
        if data_type == "candles" and not str(requirement.get("timeframe") or "").strip():
            raise ContractValidationError("candle required data must declare timeframe")


def _reject_forbidden_packet_fields(value: Any, *, path: str) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            item_path = f"{path}.{key}" if path else str(key)
            if key in FORBIDDEN_SIGNAL_PACKET_FIELDS:
                raise ContractValidationError(f"forbidden signal packet field: {item_path}")
            _reject_forbidden_packet_fields(item, path=item_path)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_forbidden_packet_fields(item, path=f"{path}[{index}]")


def _validate_strategy_decision_dict(decision: dict[str, Any]) -> None:
    action = str(decision.get("action") or decision.get("trade_action") or "").upper()
    direction = str(decision.get("direction") or "").upper()
    if action not in STRATEGY_ACTIONS:
        raise ContractValidationError(f"invalid strategy action: {action}")
    if action in ENTRY_ACTIONS and direction not in {"LONG", "SHORT"}:
        raise ContractValidationError("entry decisions require LONG or SHORT direction")
    if action == "SKIP" and direction not in {"", "FLAT"}:
        raise ContractValidationError("SKIP decisions must use FLAT direction")


def _validate_position_management_decision_dict(decision: dict[str, Any]) -> None:
    action = str(decision.get("action") or "").upper()
    if action not in POSITION_MANAGEMENT_ACTIONS:
        raise ContractValidationError(f"invalid manage_position action: {action}")


def _load_module(strategy_path: Path) -> Any:
    if not strategy_path.is_file():
        raise ContractValidationError(f"strategy module not found: {strategy_path}")
    spec = importlib.util.spec_from_file_location(f"contract_strategy_{abs(hash(str(strategy_path)))}", strategy_path)
    if spec is None or spec.loader is None:
        raise ContractValidationError(f"cannot load strategy module: {strategy_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_signal_engine_spec_mapping(path_or_engine_id: str | Path) -> dict[str, Any]:
    raw = Path(path_or_engine_id)
    if raw.is_file():
        value = json.loads(raw.read_text())
        if not isinstance(value, dict):
            raise ContractValidationError(f"signal engine spec must be an object: {raw}")
        return value
    registry_path = Path("artifacts/signal_engine/engine_registry.json")
    if registry_path.is_file():
        registry = json.loads(registry_path.read_text())
        if isinstance(registry, dict) and str(path_or_engine_id) in registry and isinstance(registry[str(path_or_engine_id)], dict):
            return registry[str(path_or_engine_id)]
    raise ContractValidationError(f"signal engine spec not found: {path_or_engine_id}")


def _load_execution_bundle_mapping(path_or_bundle_id: str | Path) -> dict[str, Any]:
    raw = Path(path_or_bundle_id)
    candidates = []
    if raw.is_file():
        candidates.append(raw)
    elif raw.is_dir():
        candidates.extend([raw / "bundle.json", raw / "execution_setup.json"])
    else:
        bundle_root = Path("artifacts/execution_bundles") / str(path_or_bundle_id)
        candidates.extend([bundle_root / "bundle.json", bundle_root / "execution_setup.json"])
    for candidate in candidates:
        if not candidate.is_file():
            continue
        value = json.loads(candidate.read_text())
        if not isinstance(value, dict):
            raise ContractValidationError(f"execution bundle must be an object: {candidate}")
        if "execution_setup" in value:
            return value
        return {"execution_setup": value}
    raise ContractValidationError(f"execution bundle not found: {path_or_bundle_id}")


def _sample_decide_context() -> dict[str, Any]:
    return {
        "signal": {
            "signal_id": "contract-sample-signal",
            "asset": "SOL",
            "instrument": "SOL-USDT-SWAP",
            "timestamp": "2026-06-08T00:00:00Z",
            "payload": {"schema_version": SUPPORTED_PACKET_SCHEMA, "asset": "SOL", "timestamp": "2026-06-08T00:00:00Z"},
        },
        "runtime_mode": "backtest",
        "execution_setup": {},
        "exchange_snapshot": {},
        "portfolio_state": {},
    }


def _sample_position_context() -> dict[str, Any]:
    return {
        "runtime_mode": "live",
        "execution_setup": {},
        "exchange_snapshot": {},
        "owner_state": {},
        "position_context": {"direction": "LONG", "size": "1", "entry_price": "100"},
        "portfolio_state": {},
    }


def _require_nonempty(value: dict[str, Any], field_name: str) -> None:
    if value.get(field_name) in (None, ""):
        raise ContractValidationError(f"{field_name} is required")


def _derived_evidence(packet: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in packet.items()
        if key not in {"schema_version", "asset", "instrument", "timestamp", "active_timeframes"}
    }


def _first_present(value: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if value.get(key) not in (None, ""):
            return value[key]
    return None


def _truthy(value: Any) -> bool:
    return value is True or str(value).lower() in {"1", "true", "yes", "on"}


def _numeric(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
