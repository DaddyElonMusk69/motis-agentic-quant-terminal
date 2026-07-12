from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any, Mapping, Sequence

import pyarrow.parquet as pq

from quant_terminal_sdk.engine_contracts import (
    ContractValidationError,
    validate_signal_packet,
    validate_strategy_module,
)
from quant_terminal_sdk.market_data_reader import (
    MarketDataCandle,
    MarketDataReader,
    read_candles_from_ref,
)
from quant_terminal_worker.execution.bundle_loader import load_strategy_module
from quant_terminal_worker.ingestion.legacy_signals import build_signal_set_key
from quant_terminal_worker.ingestion.signal_pool_extension import (
    extend_signal_pool_from_local_candles,
)
from quant_terminal_worker.signal_discovery.atlas import label_fixed_r_timestamps
from quant_terminal_worker.signal_engines.runtime import (
    EngineLiveScanContext,
    apply_fixed_engine_parameters,
    resolve_signal_engine,
)


EVALUATION_SCHEMA_VERSION = "signal_discovery_engine_evaluation.v1"


def score_candidate_signals(
    *,
    signals: Sequence[Mapping[str, Any]],
    candles: Sequence[MarketDataCandle],
    strategy_module: ModuleType | Any,
    selected_target: Mapping[str, Any],
    split_windows: Mapping[str, tuple[datetime, datetime]],
    episodes_by_split: Mapping[str, Sequence[Mapping[str, Any]]],
    cadence: Mapping[str, Any],
) -> dict[str, Any]:
    if selected_target.get("entry_semantics", "next_5m_open") != "next_5m_open":
        raise ValueError("candidate evaluation requires next_5m_open entry semantics")
    decide = getattr(strategy_module, "decide", None)
    if not callable(decide):
        raise ValueError("paired strategy must expose decide(context)")

    ordered_signals = sorted(signals, key=lambda row: _coerce_timestamp(row["timestamp"]))
    timestamps = [_coerce_timestamp(row["timestamp"]) for row in ordered_signals]
    labels = label_fixed_r_timestamps(
        candles=candles,
        decision_timestamps=timestamps,
        selected_target=selected_target,
    )
    evaluated_rows: list[dict[str, Any]] = []
    for signal, label in zip(ordered_signals, labels, strict=True):
        packet = signal.get("payload")
        if not isinstance(packet, dict):
            raise ValueError(f"signal packet is missing for {signal.get('signal_id')}")
        try:
            validate_signal_packet(packet)
        except ContractValidationError as exc:
            raise ValueError(f"packet neutrality validation failed: {exc}") from exc
        runtime_signal = _canonical_runtime_signal(signal=signal, packet=packet)
        try:
            decision = decide(
                {
                    "signal": runtime_signal,
                    "runtime_mode": "stage1",
                    "parameters": {},
                    "raw_data": {},
                }
            )
        except Exception as exc:
            raise ValueError(
                f"paired strategy is incompatible with canonical wrapper for {signal.get('signal_id')}: {exc}"
            ) from exc
        predicted_direction = _normalize_strategy_direction(
            decision,
            signal_id=str(signal.get("signal_id") or "unknown"),
        )
        evaluated_rows.append(
            {
                "timestamp": _coerce_timestamp(signal["timestamp"]),
                "target": label,
                "predicted_direction": predicted_direction,
            }
        )

    training_cadence = _optional_int(cadence.get("training_dedupe_window_minutes"))
    live_cadence = _optional_int(cadence.get("live_dedupe_window_minutes"))
    cadence_parity = (
        training_cadence is not None
        and training_cadence == live_cadence
        and cadence.get("packet_metadata_parity", True) is not False
    )
    slices = {
        split: _score_slice(
            rows=[
                row
                for row in evaluated_rows
                if _as_utc(window[0]) <= row["timestamp"] <= _as_utc(window[1])
            ],
            episodes=episodes_by_split.get(split, ()),
            selected_target=selected_target,
        )
        for split, window in split_windows.items()
    }
    required_slices = [slices.get("training"), slices.get("walk_forward")]
    accepted = cadence_parity and all(
        metrics is not None
        and metrics["emitted_timestamp_count"] > 0
        and metrics["net_r_after_costs"] > 0
        for metrics in required_slices
    )
    return {
        "schema_version": EVALUATION_SCHEMA_VERSION,
        "contracts": {
            "packet_neutrality": True,
            "strategy_wrapper_compatible": True,
            "cadence_metadata_parity": cadence_parity,
        },
        "cadence": dict(cadence),
        "slices": slices,
        "accepted": accepted,
        "acceptance_rule": (
            "contract-valid candidate with cadence parity, nonempty training and walk-forward "
            "samples, and positive net R after costs in both slices"
        ),
    }


def evaluate_registered_engine(
    *,
    workspace_root: str | Path,
    repository: Any,
    session: Mapping[str, Any],
) -> dict[str, Any]:
    root = Path(workspace_root).resolve()
    engine_id = str(session.get("candidate_engine_id") or "")
    signal_set_key = str(session.get("candidate_signal_set_key") or "")
    if not engine_id or not signal_set_key:
        raise ValueError("signal discovery evaluation requires an attached engine and signal set")
    canonical_key = build_signal_set_key(
        engine_id,
        str(session["asset"]).upper(),
        f"{str(session['asset']).upper()}-{engine_id}-canonical",
    )
    if signal_set_key != canonical_key:
        raise ValueError("signal discovery evaluation requires the canonical engine signal set")
    signal_set = repository.get_signal_set(signal_set_key)
    if signal_set is None:
        raise ValueError(f"candidate canonical signal set not found: {signal_set_key}")
    if signal_set.get("signal_engine_id") != engine_id:
        raise ValueError("candidate signal set does not belong to the attached engine")

    resolved = resolve_signal_engine(
        engine_id,
        version=signal_set.get("signal_engine_version"),
        repository=repository,
        workspace_root=root,
    )
    strategy_ref = resolved.spec.code_ref.get("base_strategy_path")
    if not isinstance(strategy_ref, str) or not strategy_ref:
        raise ValueError("candidate engine registry entry requires code_ref.base_strategy_path")
    strategy_path = Path(strategy_ref)
    if not strategy_path.is_absolute():
        strategy_path = root / strategy_path
    validate_strategy_module(strategy_path)
    strategy_module = load_strategy_module(strategy_path)

    generation = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id=engine_id,
        asset=str(session["asset"]),
        target_end=_iso_z(_coerce_timestamp(session["walk_forward_end"])),
    )
    signals = repository.list_signals_for_signal_set_window(
        signal_set_key=signal_set_key,
        window_start=_iso_z(_coerce_timestamp(session["research_start"])),
        window_end=_iso_z(_coerce_timestamp(session["walk_forward_end"])),
    )

    target = session.get("frozen_target") or {}
    selected_target = target.get("selected_target") or {}
    source_data = target.get("source_data") or {}
    if not source_data or source_data.get("dataset_id") != session.get("dataset_id"):
        raise ValueError("frozen target source dataset does not match the discovery session")
    read_end = _coerce_timestamp(session["walk_forward_end"]) + timedelta(
        hours=float(selected_target["horizon_hours"]),
        minutes=int(selected_target["entry_delay_minutes"]),
    )
    candles = read_candles_from_ref(
        dict(source_data),
        workspace_root=root,
        start=session["research_start"],
        end=read_end,
    )
    artifact_root = _artifact_root(session=session, workspace_root=root)
    bracket_contract = target.get("bracket_policy") or {}
    training_episode_path = (
        artifact_root / str(bracket_contract["training_brackets_path"])
        if bracket_contract
        else artifact_root / "atlas" / "training_episodes.parquet"
    )
    walk_forward_episode_path = (
        artifact_root / "walk_forward" / "walk_forward_brackets.parquet"
        if bracket_contract
        else artifact_root / "walk_forward" / "walk_forward_episodes.parquet"
    )
    episodes_by_split = {
        "training": _read_selected_episodes(
            training_episode_path,
            selected_target=(None if bracket_contract else selected_target),
        ),
        "walk_forward": _read_selected_episodes(
            walk_forward_episode_path,
            selected_target=None,
        ),
    }
    effective_parameters = _effective_engine_parameters(
        spec=resolved.spec,
        signal_set=signal_set,
    )
    cadence = _evaluate_cadence_contract(
        resolved=resolved,
        repository=repository,
        workspace_root=root,
        session=session,
        signals=signals,
        effective_parameters=effective_parameters,
    )
    scored = score_candidate_signals(
        signals=signals,
        candles=candles,
        strategy_module=strategy_module,
        selected_target=selected_target,
        split_windows={
            "training": (
                _coerce_timestamp(session["research_start"]),
                _coerce_timestamp(session["research_end"]),
            ),
            "walk_forward": (
                _coerce_timestamp(session["walk_forward_start"]),
                _coerce_timestamp(session["walk_forward_end"]),
            ),
        },
        episodes_by_split=episodes_by_split,
        cadence=cadence,
    )
    evaluation = {
        **scored,
        "session_id": str(session["session_id"]),
        "target_config_hash": str(target.get("config_hash") or ""),
        "signal_engine_id": engine_id,
        "signal_engine_version": resolved.spec.version,
        "signal_set_key": signal_set_key,
        "paired_strategy_path": str(strategy_path),
        "generation": generation,
    }
    evaluation_path = artifact_root / "evaluation" / "engine_evaluation.json"
    _atomic_write_json(evaluation_path, evaluation)
    return {**evaluation, "evaluation_path": str(evaluation_path)}


def _score_slice(
    *,
    rows: Sequence[Mapping[str, Any]],
    episodes: Sequence[Mapping[str, Any]],
    selected_target: Mapping[str, Any],
) -> dict[str, Any]:
    target_counts = {label: 0 for label in ("LONG", "SHORT", "NEUTRAL", "AMBIGUOUS")}
    decision_counts = {label: 0 for label in ("LONG", "SHORT", "NEUTRAL")}
    outcome_counts = {label: 0 for label in ("TP", "SL", "TIMEOUT", "AMBIGUOUS")}
    qualifying = 0
    directional_matches = 0
    directional_denominator = 0
    entered_count = 0
    net_r = 0.0
    risk_pct = float(selected_target["selected_risk_pct"])
    reward_multiple = float(selected_target["reward_multiple"])
    stop_multiple = float(selected_target["stop_multiple"])
    cost_in_r = (
        2
        * (
            float(selected_target.get("fee_bps_per_side") or 0.0)
            + float(selected_target.get("slippage_bps_per_side") or 0.0)
        )
        / 100
        / risk_pct
    )
    for row in rows:
        target = row["target"]
        target_label = str(target["label"]).upper()
        predicted = str(row["predicted_direction"]).upper()
        timestamp = _coerce_timestamp(row["timestamp"])
        matching_episode = next(
            (
                episode
                for episode in episodes
                if _coerce_timestamp(episode["start_ts"])
                <= timestamp
                <= _coerce_timestamp(episode["end_ts"])
            ),
            None,
        )
        approved_direction = (
            str(matching_episode.get("direction") or "").upper()
            if matching_episode is not None
            else None
        )
        target_counts[target_label] += 1
        decision_counts[predicted] += 1
        if approved_direction in {"LONG", "SHORT"} and predicted == approved_direction:
            qualifying += 1
        if target_label != "AMBIGUOUS":
            directional_denominator += 1
            directional_matches += int(predicted == approved_direction)
        if predicted not in {"LONG", "SHORT"}:
            continue
        entered_count += 1
        path = target[predicted.lower()]
        outcome = str(path["outcome"]).upper()
        outcome_counts[outcome] += 1
        if outcome == "TP":
            net_r += reward_multiple - cost_in_r
        elif outcome == "TIMEOUT":
            net_r += float(path["terminal_return_pct"]) / risk_pct - cost_in_r
        else:
            net_r -= stop_multiple + cost_in_r

    recalled_episodes = sum(
        1
        for episode in episodes
        if any(
            _coerce_timestamp(episode["start_ts"])
            <= _coerce_timestamp(row["timestamp"])
            <= _coerce_timestamp(episode["end_ts"])
            and str(row["predicted_direction"]).upper()
            == str(episode.get("direction") or "").upper()
            for row in rows
        )
    )
    emitted_count = len(rows)
    return {
        "emitted_timestamp_count": emitted_count,
        "target_label_counts": target_counts,
        "strategy_decision_counts": decision_counts,
        "opportunity_precision": qualifying / emitted_count if emitted_count else 0.0,
        "episode_count": len(episodes),
        "recalled_episode_count": recalled_episodes,
        "episode_recall": recalled_episodes / len(episodes) if episodes else 0.0,
        "directional_accuracy": (
            directional_matches / directional_denominator if directional_denominator else 0.0
        ),
        "entered_count": entered_count,
        "chosen_path_outcome_counts": outcome_counts,
        "cost_in_r_per_entry": cost_in_r,
        "net_r_after_costs": net_r,
        "expected_net_r_per_emitted_timestamp": net_r / emitted_count if emitted_count else 0.0,
    }


def _canonical_runtime_signal(
    *,
    signal: Mapping[str, Any],
    packet: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "signal_id": str(signal.get("signal_id") or ""),
        "signal_set_key": signal.get("signal_set_key"),
        "signal_engine_id": signal.get("signal_engine_id"),
        "asset": signal.get("asset") or packet.get("asset"),
        "instrument": signal.get("instrument") or packet.get("instrument"),
        "timestamp": signal.get("timestamp") or packet.get("timestamp"),
        "payload_schema": signal.get("payload_schema") or packet.get("schema_version"),
        "payload": dict(packet),
    }


def _normalize_strategy_direction(decision: Any, *, signal_id: str) -> str:
    if not isinstance(decision, Mapping):
        raise ValueError(f"strategy decision must be an object for {signal_id}")
    action = str(decision.get("trade_action") or decision.get("action") or "").upper()
    direction = str(decision.get("direction") or "").upper()
    if action in {"SKIP", "BLOCKED"}:
        return "NEUTRAL"
    if action not in {"ENTER", "ENTER_LONG", "ENTER_SHORT"}:
        raise ValueError(f"invalid strategy action for {signal_id}: {action}")
    if direction not in {"LONG", "SHORT"}:
        if action == "ENTER_LONG":
            return "LONG"
        if action == "ENTER_SHORT":
            return "SHORT"
        raise ValueError(f"entry decision requires LONG or SHORT for {signal_id}")
    return direction


def _effective_engine_parameters(*, spec: Any, signal_set: Mapping[str, Any]) -> dict[str, Any]:
    schema = spec.configuration_schema if isinstance(spec.configuration_schema, dict) else {}
    defaults = schema.get("default_parameters") if isinstance(schema.get("default_parameters"), dict) else {}
    manifest = signal_set.get("manifest") if isinstance(signal_set.get("manifest"), dict) else {}
    parameters = manifest.get("parameters") if isinstance(manifest.get("parameters"), dict) else {}
    return apply_fixed_engine_parameters(spec, {**defaults, **parameters})


def _evaluate_cadence_contract(
    *,
    resolved: Any,
    repository: Any,
    workspace_root: Path,
    session: Mapping[str, Any],
    signals: Sequence[Mapping[str, Any]],
    effective_parameters: Mapping[str, Any],
) -> dict[str, Any]:
    expected = _optional_int(effective_parameters.get("dedupe_window_minutes"))
    observed_training = sorted(
        {
            value
            for signal in signals
            if isinstance(signal.get("payload"), Mapping)
            for value in [
                _optional_int(
                    (signal["payload"].get("evidence") or {}).get("dedupe_window_minutes")
                    if isinstance(signal["payload"].get("evidence"), Mapping)
                    else None
                )
            ]
            if value is not None
        }
    )
    route = {
        "route_id": f"discovery-evaluation-{session['session_id']}",
        "signal_engine_id": resolved.spec.signal_engine_id,
        "signal_engine_version": resolved.spec.version,
        "asset": str(session["asset"]),
        "instrument": str(session["instrument"]),
        "active_bundle": {"execution_setup": {"engine_parameters": dict(effective_parameters)}},
    }
    live_result = resolved.scan_live_signal(
        EngineLiveScanContext(
            asset=str(session["asset"]),
            instrument=str(session["instrument"]),
            route=route,
            parameters=dict(effective_parameters),
            market_data_reader=MarketDataReader(
                repository=repository,
                workspace_root=workspace_root,
            ),
            spec=resolved.spec,
            workspace_root=workspace_root,
            repository=repository,
        )
    )
    observed_live = None
    if live_result.signal is not None:
        packet = live_result.signal.to_mapping()
        try:
            validate_signal_packet(packet)
        except ContractValidationError as exc:
            raise ValueError(f"live packet neutrality validation failed: {exc}") from exc
        evidence = packet.get("evidence") if isinstance(packet.get("evidence"), Mapping) else {}
        observed_live = _optional_int(evidence.get("dedupe_window_minutes"))
    metadata_values = [*observed_training]
    if observed_live is not None:
        metadata_values.append(observed_live)
    metadata_parity = expected is not None and all(value == expected for value in metadata_values)
    return {
        "training_dedupe_window_minutes": expected,
        "live_dedupe_window_minutes": expected if metadata_parity else observed_live,
        "observed_training_packet_minutes": observed_training,
        "observed_live_packet_minutes": observed_live,
        "live_scan_status": live_result.status,
        "packet_metadata_parity": metadata_parity,
        "effective_parameters_hash": _sha256_payload(effective_parameters),
    }


def _read_selected_episodes(
    path: Path,
    *,
    selected_target: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    if not path.is_file():
        raise ValueError(f"discovery episode artifact not found: {path}")
    rows = pq.read_table(path).to_pylist()
    if selected_target is None:
        return rows
    risk = float(selected_target["selected_risk_pct"])
    delay = int(selected_target["entry_delay_minutes"])
    horizon = float(selected_target["horizon_hours"])
    return [
        row
        for row in rows
        if float(row.get("risk_pct", risk)) == risk
        and int(row.get("entry_delay_minutes", delay)) == delay
        and float(row.get("horizon_hours", horizon)) == horizon
    ]


def _artifact_root(*, session: Mapping[str, Any], workspace_root: Path) -> Path:
    path = Path(str(session["artifact_root"]))
    return path if path.is_absolute() else workspace_root / path


def _atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    try:
        temp_path.write_text(json.dumps(value, indent=2, sort_keys=True, default=_json_default) + "\n")
        temp_path.replace(path)
    finally:
        temp_path.unlink(missing_ok=True)


def _sha256_payload(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return _iso_z(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _coerce_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str):
        return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    raise ValueError(f"invalid timestamp: {value!r}")


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _iso_z(value: datetime) -> str:
    return _as_utc(value).isoformat().replace("+00:00", "Z")
