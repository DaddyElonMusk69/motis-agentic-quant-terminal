from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_discovery.atlas import (
    label_fixed_r_timestamps,
    max_opportunity_gap_minutes,
)

BRACKET_POLICY_SCHEMA = "signal_discovery_bracket_policy.v1"
_CADENCE = timedelta(minutes=5)


def normalize_bracket_policy(policy: Mapping[str, Any]) -> dict[str, Any]:
    normalized = {
        "schema_version": BRACKET_POLICY_SCHEMA,
        "risk_pct": float(policy["risk_pct"]),
        "entry_delay_minutes": int(policy["entry_delay_minutes"]),
        "horizon_hours": float(policy["horizon_hours"]),
        "require_r_stability": bool(policy.get("require_r_stability", False)),
        "r_stability_radius": int(policy.get("r_stability_radius", 1)),
        "require_delay_stability": bool(policy.get("require_delay_stability", False)),
        "delay_agreement_pct": int(policy.get("delay_agreement_pct", 100)),
        "bridge_neutral_gap_intervals": int(
            policy.get("bridge_neutral_gap_intervals", 0)
        ),
        "minimum_persistence_timestamps": int(
            policy.get("minimum_persistence_timestamps", 1)
        ),
        "one_active_opportunity": bool(policy.get("one_active_opportunity", False)),
    }
    if normalized["risk_pct"] <= 0:
        raise ValueError("bracket policy risk_pct must be positive")
    if normalized["entry_delay_minutes"] < 0:
        raise ValueError("bracket policy entry delay must be nonnegative")
    if normalized["horizon_hours"] <= 0:
        raise ValueError("bracket policy horizon must be positive")
    if not 1 <= normalized["r_stability_radius"] <= 3:
        raise ValueError("R stability radius must be between 1 and 3")
    if not 50 <= normalized["delay_agreement_pct"] <= 100:
        raise ValueError("delay agreement must be between 50 and 100 percent")
    if not 0 <= normalized["bridge_neutral_gap_intervals"] <= 12:
        raise ValueError("neutral gap intervals must be between 0 and 12")
    if not 1 <= normalized["minimum_persistence_timestamps"] <= 24:
        raise ValueError("minimum persistence must be between 1 and 24")
    return normalized


def build_bracket_preview(
    *,
    labels: Sequence[Mapping[str, Any]],
    risk_values: Sequence[float],
    entry_delays: Sequence[int],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    normalized = normalize_bracket_policy(policy)
    risks = sorted({float(value) for value in risk_values})
    delays = sorted({int(value) for value in entry_delays})
    selected_risk = normalized["risk_pct"]
    selected_delay = normalized["entry_delay_minutes"]
    horizon = normalized["horizon_hours"]
    if selected_risk not in risks:
        raise ValueError("selected bracket risk is not in the atlas grid")
    if selected_delay not in delays:
        raise ValueError("selected bracket delay is not in the atlas grid")
    if normalized["require_r_stability"] and len(risks) < 2:
        raise ValueError("R stability requires at least two stop distances")
    if normalized["require_delay_stability"] and len(delays) < 2:
        raise ValueError("delay stability requires at least two entry delays")

    indexed = {
        (
            float(row["risk_pct"]),
            int(row["scenario_entry_delay_minutes"]),
            float(row["scenario_horizon_hours"]),
            _as_utc(row["decision_ts"]),
        ): row
        for row in labels
        if float(row["scenario_horizon_hours"]) == horizon
    }
    primary = sorted(
        (
            row
            for key, row in indexed.items()
            if key[0] == selected_risk and key[1] == selected_delay and key[2] == horizon
        ),
        key=lambda row: _as_utc(row["decision_ts"]),
    )
    if not primary:
        raise ValueError("selected bracket scenario has no training labels")

    raw_states = [_state(row=row, direction=_direction(row), bridgeable=False) for row in primary]
    raw_brackets = _states_to_brackets(raw_states, normalized)
    neighbor_risks = _neighbor_values(
        risks,
        selected_risk,
        radius=normalized["r_stability_radius"],
    )
    required_delay_matches = max(
        2,
        math.ceil(len(delays) * normalized["delay_agreement_pct"] / 100),
    )
    states: list[dict[str, Any]] = []
    stability_rejected = 0
    for row in primary:
        timestamp = _as_utc(row["decision_ts"])
        direction = _direction(row)
        survives = direction is not None
        if survives and normalized["require_r_stability"]:
            survives = all(
                _direction(indexed.get((risk, selected_delay, horizon, timestamp), {}))
                == direction
                for risk in neighbor_risks
            )
        if survives and normalized["require_delay_stability"]:
            matching_delays = sum(
                _direction(indexed.get((selected_risk, delay, horizon, timestamp), {}))
                == direction
                for delay in delays
            )
            survives = matching_delays >= required_delay_matches
        if direction is not None and not survives:
            stability_rejected += 1
        label = str(row.get("label") or "").upper()
        states.append(
            _state(
                row=row,
                direction=direction if survives else None,
                bridgeable=label == "NEUTRAL",
            )
        )

    merged_gap_count = _bridge_neutral_gaps(
        states,
        max_gap=normalized["bridge_neutral_gap_intervals"],
    )
    brackets = _states_to_brackets(states, normalized)
    minimum = normalized["minimum_persistence_timestamps"]
    persistence_removed = sum(1 for row in brackets if row["timestamp_count"] < minimum)
    brackets = [row for row in brackets if row["timestamp_count"] >= minimum]
    overlap_suppressed = 0
    if normalized["one_active_opportunity"]:
        kept: list[dict[str, Any]] = []
        active_until: datetime | None = None
        for bracket in brackets:
            if active_until is not None and bracket["start_ts"] < active_until:
                overlap_suppressed += 1
                continue
            kept.append(bracket)
            active_until = bracket["resolution_ts"]
        brackets = kept

    for index, bracket in enumerate(brackets, start=1):
        bracket["bracket_id"] = f"bracket-{index:06d}"
        bracket["episode_id"] = bracket["bracket_id"]
    diagnostics = _diagnostics(
        raw_brackets=raw_brackets,
        brackets=brackets,
        eligible_timestamps=[row["decision_ts"] for row in primary],
        stability_rejected=stability_rejected,
        merged_gap_count=merged_gap_count,
        persistence_removed=persistence_removed,
        overlap_suppressed=overlap_suppressed,
    )
    return {
        "schema_version": "signal_discovery_bracket_preview.v1",
        "policy": normalized,
        "policy_hash": _json_sha256(normalized),
        "brackets": brackets,
        "diagnostics": diagnostics,
    }


def load_training_bracket_preview(
    *,
    artifact_root: str | Path,
    risk_values: Sequence[float],
    entry_delays: Sequence[int],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    labels_path = Path(artifact_root) / "atlas" / "training_timestamp_labels.parquet"
    if not labels_path.is_file():
        raise ValueError("training timestamp labels are unavailable")
    labels = _read_policy_labels(
        labels_path=labels_path,
        risk_values=risk_values,
        entry_delays=entry_delays,
        policy=policy,
    )
    return build_bracket_preview(
        labels=labels,
        risk_values=risk_values,
        entry_delays=entry_delays,
        policy=policy,
    )


def approve_training_brackets(
    *,
    artifact_root: str | Path,
    session_id: str,
    risk_values: Sequence[float],
    entry_delays: Sequence[int],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    root = Path(artifact_root)
    labels_path = root / "atlas" / "training_timestamp_labels.parquet"
    preview = load_training_bracket_preview(
        artifact_root=root,
        risk_values=risk_values,
        entry_delays=entry_delays,
        policy=policy,
    )
    if not preview["brackets"]:
        raise ValueError("bracket cleanup produced zero brackets")
    approval_root = root / "brackets"
    policy_path = approval_root / "bracket_policy.json"
    brackets_path = approval_root / "training_brackets.parquet"
    negatives_path = approval_root / "training_hard_negatives.parquet"
    existing = _read_json(policy_path) if policy_path.is_file() else {}
    revision = int(existing.get("revision") or 0) + 1
    member_timestamps = {
        timestamp
        for bracket in preview["brackets"]
        for timestamp in bracket["member_timestamps"]
    }
    selected = preview["policy"]
    primary_rows = pq.read_table(
        labels_path,
        filters=[
            ("risk_pct", "=", selected["risk_pct"]),
            ("scenario_entry_delay_minutes", "=", selected["entry_delay_minutes"]),
            ("scenario_horizon_hours", "=", selected["horizon_hours"]),
        ],
    ).to_pylist()
    removed = [
        {
            "decision_ts": _as_utc(row["decision_ts"]),
            "original_label": str(row.get("label") or "").upper(),
            "reason": "outside_approved_brackets",
        }
        for row in primary_rows
        if _direction(row) is not None and _as_utc(row["decision_ts"]) not in member_timestamps
    ]
    _write_parquet(brackets_path, preview["brackets"])
    _write_parquet(negatives_path, removed)
    contract = {
        "schema_version": BRACKET_POLICY_SCHEMA,
        "session_id": session_id,
        "revision": revision,
        "policy": preview["policy"],
        "policy_hash": preview["policy_hash"],
        "source_atlas_path": str(labels_path.relative_to(root)),
        "source_atlas_hash": _file_sha256(labels_path),
        "training_brackets_path": str(brackets_path.relative_to(root)),
        "training_brackets_hash": _file_sha256(brackets_path),
        "training_hard_negatives_path": str(negatives_path.relative_to(root)),
        "training_hard_negatives_hash": _file_sha256(negatives_path),
        "diagnostics": preview["diagnostics"],
    }
    _write_json(policy_path, contract)
    return {**preview, "approval": contract}


def read_approved_bracket_contract(*, artifact_root: str | Path) -> dict[str, Any]:
    path = Path(artifact_root) / "brackets" / "bracket_policy.json"
    if not path.is_file():
        raise ValueError("approved bracket policy does not exist")
    contract = _read_json(path)
    root = Path(artifact_root)
    for path_key, hash_key in (
        ("source_atlas_path", "source_atlas_hash"),
        ("training_brackets_path", "training_brackets_hash"),
        ("training_hard_negatives_path", "training_hard_negatives_hash"),
    ):
        artifact = root / str(contract[path_key])
        if not artifact.is_file() or _file_sha256(artifact) != contract[hash_key]:
            raise ValueError("approved bracket artifact fingerprint changed")
    return contract


def apply_bracket_policy(
    *,
    labels: Sequence[Mapping[str, Any]],
    risk_values: Sequence[float],
    entry_delays: Sequence[int],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    return build_bracket_preview(
        labels=labels,
        risk_values=risk_values,
        entry_delays=entry_delays,
        policy=policy,
    )


def build_policy_scenario_labels(
    *,
    candles: Sequence[MarketDataCandle],
    decision_timestamps: Sequence[datetime],
    selected_target: Mapping[str, Any],
    risk_values: Sequence[float],
    entry_delays: Sequence[int],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    normalized = normalize_bracket_policy(policy)
    selected_risk = float(selected_target["selected_risk_pct"])
    selected_delay = int(selected_target["entry_delay_minutes"])
    required_risks = (
        [
            selected_risk,
            *_neighbor_values(
                sorted({float(value) for value in risk_values}),
                selected_risk,
                radius=normalized["r_stability_radius"],
            ),
        ]
        if normalized["require_r_stability"]
        else [selected_risk]
    )
    required_delays = (
        sorted({int(value) for value in entry_delays})
        if normalized["require_delay_stability"]
        else [selected_delay]
    )
    rows: list[dict[str, Any]] = []
    for risk in sorted(set(required_risks)):
        for delay in required_delays:
            target = {
                **selected_target,
                "selected_risk_pct": risk,
                "entry_delay_minutes": delay,
            }
            for row in label_fixed_r_timestamps(
                candles=candles,
                decision_timestamps=decision_timestamps,
                selected_target=target,
            ):
                row["scenario_entry_delay_minutes"] = delay
                row["scenario_horizon_hours"] = float(selected_target["horizon_hours"])
                rows.append(row)
    return rows


def _neighbor_values(
    values: Sequence[float], selected: float, *, radius: int = 1
) -> list[float]:
    index = list(values).index(selected)
    return list(values[max(0, index - radius) : index]) + list(
        values[index + 1 : index + radius + 1]
    )


def _read_policy_labels(
    *,
    labels_path: Path,
    risk_values: Sequence[float],
    entry_delays: Sequence[int],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    normalized = normalize_bracket_policy(policy)
    risks = sorted({float(value) for value in risk_values})
    delays = sorted({int(value) for value in entry_delays})
    selected_risk = normalized["risk_pct"]
    selected_delay = normalized["entry_delay_minutes"]
    required_risks = (
        [
            selected_risk,
            *_neighbor_values(
                risks,
                selected_risk,
                radius=normalized["r_stability_radius"],
            ),
        ]
        if normalized["require_r_stability"]
        else [selected_risk]
    )
    required_delays = delays if normalized["require_delay_stability"] else [selected_delay]
    filters = [
        [
            ("risk_pct", "=", risk),
            ("scenario_entry_delay_minutes", "=", delay),
            ("scenario_horizon_hours", "=", normalized["horizon_hours"]),
        ]
        for risk in sorted(set(required_risks))
        for delay in required_delays
    ]
    return pq.read_table(labels_path, filters=filters).to_pylist()


def _direction(row: Mapping[str, Any]) -> str | None:
    label = str(row.get("label") or "").upper()
    return label if label in {"LONG", "SHORT"} else None


def _state(
    *, row: Mapping[str, Any], direction: str | None, bridgeable: bool
) -> dict[str, Any]:
    return {
        "row": row,
        "timestamp": _as_utc(row["decision_ts"]),
        "direction": direction,
        "bridgeable": bridgeable,
        "inherited": False,
    }


def _bridge_neutral_gaps(states: list[dict[str, Any]], *, max_gap: int) -> int:
    if max_gap <= 0:
        return 0
    merged = 0
    index = 1
    while index < len(states) - 1:
        if not states[index]["bridgeable"] or states[index]["direction"] is not None:
            index += 1
            continue
        gap_start = index
        while index < len(states) and states[index]["bridgeable"]:
            if states[index]["timestamp"] != states[index - 1]["timestamp"] + _CADENCE:
                break
            index += 1
        gap_end = index
        gap_size = gap_end - gap_start
        left = states[gap_start - 1]
        right = states[gap_end] if gap_end < len(states) else None
        continuous_right = (
            right is not None
            and right["timestamp"] == states[gap_end - 1]["timestamp"] + _CADENCE
        )
        if (
            gap_size <= max_gap
            and continuous_right
            and left["direction"] is not None
            and left["direction"] == right["direction"]
        ):
            for gap_state in states[gap_start:gap_end]:
                gap_state["direction"] = left["direction"]
                gap_state["inherited"] = True
            merged += 1
    return merged


def _states_to_brackets(
    states: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    brackets: list[dict[str, Any]] = []
    current: list[Mapping[str, Any]] = []
    for state in states:
        direction = state["direction"]
        if direction is None:
            if current:
                brackets.append(_bracket(current, policy))
                current = []
            continue
        if current and (
            direction != current[-1]["direction"]
            or state["timestamp"] != current[-1]["timestamp"] + _CADENCE
        ):
            brackets.append(_bracket(current, policy))
            current = []
        current.append(state)
    if current:
        brackets.append(_bracket(current, policy))
    return brackets


def _bracket(states: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]) -> dict[str, Any]:
    start = states[0]["timestamp"]
    end = states[-1]["timestamp"]
    anchor = next(state for state in states if not state["inherited"])
    row = anchor["row"]
    direction = str(anchor["direction"])
    path = row.get(direction.lower()) or {}
    resolution = path.get("first_touch_ts") or row.get("horizon_end_ts")
    return {
        "bracket_id": "",
        "episode_id": "",
        "direction": direction,
        "start_ts": start,
        "end_ts": end,
        "timestamp_count": len(states),
        "duration_minutes": int((end - start).total_seconds() // 60),
        "member_timestamps": [state["timestamp"] for state in states],
        "inherited_timestamp_count": sum(int(state["inherited"]) for state in states),
        "resolution_ts": _as_utc(resolution),
        "risk_pct": float(policy["risk_pct"]),
        "entry_delay_minutes": int(policy["entry_delay_minutes"]),
        "horizon_hours": float(policy["horizon_hours"]),
    }


def _diagnostics(
    *,
    raw_brackets: Sequence[Mapping[str, Any]],
    brackets: Sequence[Mapping[str, Any]],
    eligible_timestamps: Sequence[Any],
    stability_rejected: int,
    merged_gap_count: int,
    persistence_removed: int,
    overlap_suppressed: int,
) -> dict[str, Any]:
    def counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
        return {
            direction: sum(1 for row in rows if row["direction"] == direction)
            for direction in ("LONG", "SHORT")
        }

    def monthly_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
        result: dict[str, int] = {}
        for row in rows:
            month = row["start_ts"].strftime("%Y-%m")
            result[month] = result.get(month, 0) + 1
        return result
    raw_total = len(raw_brackets)
    preview_total = len(brackets)
    return {
        "raw_total_brackets": raw_total,
        "preview_total_brackets": preview_total,
        "raw_max_opportunity_gap_minutes": max_opportunity_gap_minutes(
            eligible_timestamps=eligible_timestamps,
            brackets=raw_brackets,
        ),
        "max_opportunity_gap_minutes": max_opportunity_gap_minutes(
            eligible_timestamps=eligible_timestamps,
            brackets=brackets,
        ),
        "raw_direction_counts": counts(raw_brackets),
        "preview_direction_counts": counts(brackets),
        "removed_bracket_count": max(0, raw_total - preview_total),
        "stability_rejected_timestamp_count": stability_rejected,
        "merged_gap_count": merged_gap_count,
        "persistence_removed_count": persistence_removed,
        "overlap_suppressed_count": overlap_suppressed,
        "raw_monthly_bracket_counts": monthly_counts(raw_brackets),
        "monthly_bracket_counts": monthly_counts(brackets),
    }


def _as_utc(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _json_sha256(value: Mapping[str, Any]) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default)
    return hashlib.sha256(payload.encode()).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_parquet(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f"{path.name}.tmp")
    table = pa.Table.from_pylist([_arrow_value(dict(row)) for row in rows])
    pq.write_table(table, temp)
    temp.replace(path)


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f"{path.name}.tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True, default=_json_default) + "\n")
    temp.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return _as_utc(value).isoformat().replace("+00:00", "Z")
    raise TypeError(f"cannot encode {type(value).__name__}")


def _arrow_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _arrow_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_arrow_value(item) for item in value]
    return value
