from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from quant_terminal_sdk.market_data_reader import read_candles_from_ref
from quant_terminal_worker.signal_discovery.atlas import label_fixed_r_timestamps


LABEL_CONTRACT = "fixed_r_first_touch.v1"


def handoff_accepted_candidate(
    *,
    workspace_root: str | Path,
    repository: Any,
    session: Mapping[str, Any],
) -> dict[str, Any]:
    evaluation = session.get("evaluation") or {}
    if not evaluation.get("accepted"):
        raise ValueError("signal discovery handoff requires an accepted evaluation")
    target_contract = session.get("frozen_target") or {}
    selected_target = target_contract.get("selected_target") or {}
    source_data = target_contract.get("source_data") or {}
    if source_data.get("dataset_id") != session.get("dataset_id"):
        raise ValueError("frozen target source dataset does not match the discovery session")
    engine_id = str(session.get("candidate_engine_id") or "")
    signal_set_key = str(session.get("candidate_signal_set_key") or "")
    if not engine_id or not signal_set_key:
        raise ValueError("signal discovery handoff requires an attached engine and signal set")
    signal_set = repository.get_signal_set(signal_set_key)
    if signal_set is None:
        raise ValueError(f"candidate signal set not found: {signal_set_key}")
    if signal_set.get("signal_engine_id") != engine_id:
        raise ValueError("candidate signal set does not belong to the attached engine")

    root = Path(workspace_root).resolve()
    artifact_root = _artifact_root(session=session, workspace_root=root)
    stage0_root = artifact_root / "handoff" / "stage0"
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    packet_root = stage0_root / "scores" / "_scoreable_signal_subset" / "packets"
    ground_truth_root.mkdir(parents=True, exist_ok=True)
    packet_root.mkdir(parents=True, exist_ok=True)
    for stale_path in (*ground_truth_root.glob("*.json"), *packet_root.glob("*.json")):
        stale_path.unlink()

    signals = repository.list_signals_for_signal_set_window(
        signal_set_key=signal_set_key,
        window_start=_iso_z(_coerce_timestamp(session["research_start"])),
        window_end=_iso_z(_coerce_timestamp(session["walk_forward_end"])),
    )
    if not signals:
        raise ValueError("candidate signal set has no packets in the frozen discovery windows")
    timestamps = [_coerce_timestamp(signal["timestamp"]) for signal in signals]
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
    labels = label_fixed_r_timestamps(
        candles=candles,
        decision_timestamps=timestamps,
        selected_target=selected_target,
    )

    records: list[dict[str, Any]] = []
    artifact_records: list[dict[str, str]] = []
    used_names: set[str] = set()
    for signal, label in zip(signals, labels, strict=True):
        signal_id = str(signal["signal_id"])
        file_stem = _unique_file_stem(signal_id=signal_id, used_names=used_names)
        record = _ground_truth_record(
            signal=signal,
            label=label,
            session=session,
            target_contract=target_contract,
        )
        ground_truth_path = ground_truth_root / f"{file_stem}.json"
        packet_path = packet_root / f"{file_stem}.json"
        _atomic_write_json(ground_truth_path, record)
        packet = signal.get("payload")
        if not isinstance(packet, Mapping):
            raise ValueError(f"candidate signal is missing packet payload: {signal_id}")
        _atomic_write_json(packet_path, dict(packet))
        records.append(record)
        artifact_records.append(
            {
                "signal_id": signal_id,
                "ground_truth_path": str(ground_truth_path),
                "packet_path": str(packet_path),
            }
        )

    target_path = stage0_root / "scores" / "fixed_target_contract.json"
    _atomic_write_json(target_path, dict(target_contract))
    direction_counts = {
        label: sum(1 for record in records if record["natural_direction"] == label)
        for label in ("AMBIGUOUS", "LONG", "NEUTRAL", "SHORT")
    }
    qualifying_count = direction_counts["LONG"] + direction_counts["SHORT"]
    trigger_rate_pct = round(100 * qualifying_count / len(records), 6)
    summary_path = stage0_root / "scores" / "ground_truth_summary.json"
    summary = {
        "schema_version": "signal_discovery_stage0_handoff.v1",
        "label_contract": LABEL_CONTRACT,
        "source_discovery_session_id": str(session["session_id"]),
        "target_config_hash": str(target_contract.get("config_hash") or ""),
        "fixed_target_contract_path": str(target_path),
        "metrics": {
            "total_records": len(records),
            "direction_counts": direction_counts,
            "qualifying_records": qualifying_count,
            "trigger_rate_pct": trigger_rate_pct,
            "status_counts": {
                "triggered": qualifying_count,
                "no_trigger": direction_counts["NEUTRAL"],
                "ambiguous": direction_counts["AMBIGUOUS"],
            },
            "branch_path": "path_a",
            "branch_decision": "fixed_target_discovery_go_to_stage1a",
        },
        "artifacts": artifact_records,
    }
    _atomic_write_json(summary_path, summary)

    target_version = int(session.get("target_version") or target_contract.get("target_version") or 1)
    universe_run_id = f"stage0-discovery-{session['session_id']}-v{target_version}"
    signal_set_id = str(signal_set["signal_set_id"])
    candidate_id = f"{universe_run_id}:{engine_id}:{str(session['asset']).upper()}:{signal_set_id}"
    target_pct = float(selected_target["selected_risk_pct"]) * float(
        selected_target["reward_multiple"]
    )
    stop_pct = float(selected_target["selected_risk_pct"]) * float(
        selected_target["stop_multiple"]
    )
    metrics = {
        "artifact_root": str(stage0_root),
        "label_contract": LABEL_CONTRACT,
        "label_mode": LABEL_CONTRACT,
        "target_pct": target_pct,
        "stop_pct": stop_pct,
        "forward_hours": int(selected_target["horizon_hours"]),
        "entry_delay_minutes": int(selected_target["entry_delay_minutes"]),
        "entry_semantics": selected_target.get("entry_semantics"),
        "source_discovery_session_id": str(session["session_id"]),
        "target_config_hash": str(target_contract.get("config_hash") or ""),
        "ground_truth_summary_path": str(summary_path),
        "ground_truth_root": str(ground_truth_root),
        "fixed_target_contract_path": str(target_path),
        "evaluation_path": evaluation.get("evaluation_path"),
        "trigger_rate_pct": trigger_rate_pct,
        "total_records": len(records),
        "direction_counts": direction_counts,
    }
    run = {
        "universe_run_id": universe_run_id,
        "name": f"Outcome-First {str(session['asset']).upper()} v{target_version}",
        "config_hash": _handoff_config_hash(
            session_id=str(session["session_id"]),
            target_config_hash=str(target_contract.get("config_hash") or ""),
            signal_set_key=signal_set_key,
            target_version=target_version,
        ),
        "window_start": session["research_start"],
        "window_end": session["walk_forward_end"],
        "train_start": _coerce_timestamp(session["research_start"]).date(),
        "train_end": _coerce_timestamp(session["research_end"]).date(),
        "walk_forward_start": _coerce_timestamp(session["walk_forward_start"]).date(),
        "walk_forward_end": _coerce_timestamp(session["walk_forward_end"]).date(),
        "forward_hours": int(selected_target["horizon_hours"]),
        "trigger_rate_threshold_pct": 0.0,
        "engine_filter": [engine_id],
        "status": "completed",
        "summary": {
            "total_candidates": 1,
            "accepted": 1,
            "source": "signal_discovery_fixed_target",
            "source_discovery_session_id": str(session["session_id"]),
        },
    }
    candidate = {
        "candidate_id": candidate_id,
        "universe_run_id": universe_run_id,
        "signal_set_key": signal_set_key,
        "signal_engine_id": engine_id,
        "signal_engine_version": str(signal_set.get("signal_engine_version") or "unknown"),
        "asset": str(session["asset"]).upper(),
        "signal_set_id": signal_set_id,
        "packet_count": len(records),
        "trigger_rate_pct": trigger_rate_pct,
        "branch_path": "path_a",
        "acceptance_status": "accepted",
        "duplicate_status": "new",
        "existing_strategy_id": None,
        "last_error": {},
        "metrics": metrics,
    }
    repository.create_stage0_universe(run, [candidate])
    refresh_summary = getattr(repository, "refresh_stage0_universe_summary", None)
    if callable(refresh_summary):
        refresh_summary(universe_run_id)
    stored_candidate = repository.get_stage0_universe_candidate(candidate_id)
    if stored_candidate is None:
        raise RuntimeError("signal discovery Stage 0 candidate was not persisted")
    return {
        "schema_version": "signal_discovery_handoff.v1",
        "session_id": str(session["session_id"]),
        "universe_run_id": universe_run_id,
        "candidate_id": candidate_id,
        "stage0_artifact_root": str(stage0_root),
        "ground_truth_summary_path": str(summary_path),
        "fixed_target_contract_path": str(target_path),
        "candidate": stored_candidate,
    }


def _ground_truth_record(
    *,
    signal: Mapping[str, Any],
    label: Mapping[str, Any],
    session: Mapping[str, Any],
    target_contract: Mapping[str, Any],
) -> dict[str, Any]:
    natural_direction = str(label["label"]).upper()
    status = (
        "triggered"
        if natural_direction in {"LONG", "SHORT"}
        else "ambiguous"
        if natural_direction == "AMBIGUOUS"
        else "no_trigger"
    )
    return {
        "schema_version": "signal_discovery_ground_truth.v1",
        "label_contract": LABEL_CONTRACT,
        "signal_id": str(signal["signal_id"]),
        "timestamp": _iso_z(_coerce_timestamp(signal["timestamp"])),
        "sample_role": (
            "training"
            if _coerce_timestamp(signal["timestamp"])
            <= _coerce_timestamp(session["research_end"])
            else "walk_forward_test"
        ),
        "natural_direction": natural_direction,
        "status": status,
        "entry_ts": label["entry_ts"],
        "entry_price": label["entry_price"],
        "entry_semantics": label["entry_semantics"],
        "target_pct": float(label["risk_pct"]) * float(label["reward_multiple"]),
        "stop_pct": float(label["risk_pct"]) * float(label["stop_multiple"]),
        "forward_hours": int(float(label["horizon_hours"])),
        "long_path": label["long"],
        "short_path": label["short"],
        "source_discovery_session_id": str(session["session_id"]),
        "target_config_hash": str(target_contract.get("config_hash") or ""),
    }


def _unique_file_stem(*, signal_id: str, used_names: set[str]) -> str:
    tail = signal_id.rsplit(":", 1)[-1]
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", tail).strip("-._") or "signal"
    if stem in used_names:
        digest = hashlib.sha256(signal_id.encode()).hexdigest()[:10]
        stem = f"{stem}-{digest}"
    used_names.add(stem)
    return stem


def _handoff_config_hash(
    *,
    session_id: str,
    target_config_hash: str,
    signal_set_key: str,
    target_version: int,
) -> str:
    value = f"{session_id}|{target_config_hash}|{signal_set_key}|{target_version}"
    return hashlib.sha256(value.encode()).hexdigest()


def _atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    try:
        temp_path.write_text(
            json.dumps(value, indent=2, sort_keys=True, default=_json_default) + "\n"
        )
        temp_path.replace(path)
    finally:
        temp_path.unlink(missing_ok=True)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return _iso_z(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _artifact_root(*, session: Mapping[str, Any], workspace_root: Path) -> Path:
    path = Path(str(session["artifact_root"]))
    return path if path.is_absolute() else workspace_root / path


def _coerce_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    raise ValueError(f"invalid timestamp: {value!r}")


def _iso_z(value: datetime) -> str:
    return _coerce_timestamp(value).isoformat().replace("+00:00", "Z")
