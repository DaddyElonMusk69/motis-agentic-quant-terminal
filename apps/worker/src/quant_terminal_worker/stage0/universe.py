from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any


def build_stage0_universe(
    *,
    universe_run_id: str,
    window_start: str,
    window_end: str,
    forward_hours: int,
    trigger_rate_threshold_pct: float,
    train_start: str | None = None,
    train_end: str | None = None,
    walk_forward_start: str | None = None,
    walk_forward_end: str | None = None,
    signal_sets: list[dict[str, Any]],
    metrics_by_signal_set: dict[str, dict[str, Any]],
    existing_rnd_by_signal_set: dict[str, dict[str, Any]],
    signal_counts_by_signal_set: dict[str, int],
    split_signal_counts_by_signal_set: dict[str, dict[str, int]] | None = None,
    engine_ids: list[str] | None,
    asset_symbols: list[str] | None = None,
) -> dict[str, Any]:
    config_hash = build_stage0_universe_config_hash(
        window_start=window_start,
        window_end=window_end,
        forward_hours=forward_hours,
        trigger_rate_threshold_pct=trigger_rate_threshold_pct,
        train_start=train_start,
        train_end=train_end,
        walk_forward_start=walk_forward_start,
        walk_forward_end=walk_forward_end,
        engine_ids=engine_ids,
        asset_symbols=asset_symbols,
    )
    eligible_signal_sets = _one_pool_per_engine_asset(
        [
            signal_set
            for signal_set in signal_sets
            if _matches_universe_window(signal_set, window_start, window_end, engine_ids, asset_symbols)
            and signal_counts_by_signal_set.get(signal_set["signal_set_key"], 0) > 0
            and _has_required_split_coverage(
                    split_signal_counts_by_signal_set=split_signal_counts_by_signal_set,
                    required_splits=_required_splits(
                        train_start=train_start,
                        train_end=train_end,
                        walk_forward_start=walk_forward_start,
                        walk_forward_end=walk_forward_end,
                    ),
                    signal_set=signal_set,
                    train_start=train_start,
                    train_end=train_end,
                    walk_forward_start=walk_forward_start,
                    walk_forward_end=walk_forward_end,
                )
        ],
        signal_counts_by_signal_set=signal_counts_by_signal_set,
        split_signal_counts_by_signal_set=split_signal_counts_by_signal_set,
    )
    candidates = [
        _build_candidate(
            universe_run_id=universe_run_id,
            signal_set=signal_set,
            signal_count=signal_counts_by_signal_set[signal_set["signal_set_key"]],
            metrics=metrics_by_signal_set.get(signal_set["signal_set_key"]),
            existing_rnd=existing_rnd_by_signal_set.get(signal_set["signal_set_key"]),
            trigger_rate_threshold_pct=trigger_rate_threshold_pct,
        )
        for signal_set in eligible_signal_sets
    ]
    accepted = sum(1 for candidate in candidates if candidate["acceptance_status"] == "accepted")
    watchlist = sum(1 for candidate in candidates if candidate["acceptance_status"] == "watchlist")
    pending = sum(1 for candidate in candidates if candidate["acceptance_status"] == "pending_stage0")
    return {
        "run": {
            "universe_run_id": universe_run_id,
            "config_hash": config_hash,
            "window_start": window_start,
            "window_end": window_end,
            "train_start": train_start,
            "train_end": train_end,
            "walk_forward_start": walk_forward_start,
            "walk_forward_end": walk_forward_end,
            "forward_hours": forward_hours,
            "trigger_rate_threshold_pct": trigger_rate_threshold_pct,
            "engine_filter": engine_ids or [],
            "status": "created",
            "summary": {
                "total_candidates": len(candidates),
                "accepted": accepted,
                "watchlist": watchlist,
                "pending_stage0": pending,
            },
        },
        "candidates": candidates,
    }


def build_stage0_universe_config_hash(
    *,
    window_start: str,
    window_end: str,
    forward_hours: int,
    trigger_rate_threshold_pct: float,
    train_start: str | None = None,
    train_end: str | None = None,
    walk_forward_start: str | None = None,
    walk_forward_end: str | None = None,
    engine_ids: list[str] | None,
    asset_symbols: list[str] | None = None,
) -> str:
    payload = {
        "selection_version": "signal_window_count_v1",
        "window_start": window_start,
        "window_end": window_end,
        "forward_hours": forward_hours,
        "trigger_rate_threshold_pct": trigger_rate_threshold_pct,
        "train_start": train_start,
        "train_end": train_end,
        "walk_forward_start": walk_forward_start,
        "walk_forward_end": walk_forward_end,
        "engine_ids": sorted(engine_ids or []),
        "asset_symbols": sorted(asset_symbols or []),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()[:16]


def _build_candidate(
    *,
    universe_run_id: str,
    signal_set: dict[str, Any],
    signal_count: int,
    metrics: dict[str, Any] | None,
    existing_rnd: dict[str, Any] | None,
    trigger_rate_threshold_pct: float,
) -> dict[str, Any]:
    trigger_rate = metrics.get("trigger_rate_pct") if metrics else None
    if trigger_rate is None:
        acceptance_status = "pending_stage0"
        branch_path = "pending"
    elif trigger_rate >= trigger_rate_threshold_pct:
        acceptance_status = "accepted"
        branch_path = "path_a"
    else:
        acceptance_status = "watchlist"
        branch_path = "path_b"

    return {
        "candidate_id": f"{universe_run_id}:{signal_set['signal_set_key']}",
        "universe_run_id": universe_run_id,
        "signal_set_key": signal_set["signal_set_key"],
        "signal_engine_id": signal_set["signal_engine_id"],
        "signal_engine_version": signal_set["signal_engine_version"],
        "asset": signal_set["asset"],
        "signal_set_id": signal_set["signal_set_id"],
        "packet_count": signal_count,
        "trigger_rate_pct": trigger_rate,
        "branch_path": branch_path,
        "acceptance_status": acceptance_status,
        "duplicate_status": "existing_rnd" if existing_rnd else "new",
        "existing_strategy_id": existing_rnd.get("strategy_id") if existing_rnd else None,
        "metrics": metrics or {},
    }


def _required_splits(
    *,
    train_start: str | None,
    train_end: str | None,
    walk_forward_start: str | None,
    walk_forward_end: str | None,
) -> list[str]:
    required: list[str] = []
    if train_start and train_end:
        required.append("train")
    if walk_forward_start and walk_forward_end:
        required.append("walk_forward")
    return required


def _has_required_split_coverage(
    *,
    split_signal_counts_by_signal_set: dict[str, dict[str, int]] | None,
    required_splits: list[str],
    signal_set: dict[str, Any],
    train_start: str | None,
    train_end: str | None,
    walk_forward_start: str | None,
    walk_forward_end: str | None,
) -> bool:
    if not required_splits:
        return True
    signal_set_key = signal_set["signal_set_key"]
    split_counts = (split_signal_counts_by_signal_set or {}).get(signal_set_key, {})
    return all(split_counts.get(split, 0) > 0 for split in required_splits)


def _parse_date_window_start(value: str) -> datetime:
    if "T" in value:
        return _parse_datetime(value)
    return _parse_datetime(f"{value}T00:00:00Z")


def _parse_date_window_end(value: str) -> datetime:
    if "T" in value:
        return _parse_datetime(value)
    return _parse_datetime(f"{value}T23:59:59Z")


def _one_pool_per_engine_asset(
    signal_sets: list[dict[str, Any]],
    *,
    signal_counts_by_signal_set: dict[str, int],
    split_signal_counts_by_signal_set: dict[str, dict[str, int]] | None,
) -> list[dict[str, Any]]:
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for signal_set in signal_sets:
        key = (signal_set["signal_engine_id"], signal_set["asset"])
        current = selected.get(key)
        if current is None or _pool_rank(signal_set, signal_counts_by_signal_set, split_signal_counts_by_signal_set) > _pool_rank(
            current,
            signal_counts_by_signal_set,
            split_signal_counts_by_signal_set,
        ):
            selected[key] = signal_set
    return sorted(selected.values(), key=lambda item: (item["asset"], item["signal_engine_id"], item["signal_set_id"]))


def _pool_rank(
    signal_set: dict[str, Any],
    signal_counts_by_signal_set: dict[str, int],
    split_signal_counts_by_signal_set: dict[str, dict[str, int]] | None,
) -> tuple[int, int, datetime, str]:
    signal_set_key = signal_set["signal_set_key"]
    split_counts = (split_signal_counts_by_signal_set or {}).get(signal_set_key, {})
    split_total = sum(split_counts.values())
    overall_count = signal_counts_by_signal_set.get(signal_set_key, 0)
    end_ts = signal_set.get("coverage_end_ts") or signal_set.get("end_ts")
    parsed_end = _parse_datetime(end_ts) if end_ts else datetime.min
    return split_total, overall_count, parsed_end, signal_set["signal_set_key"]


def _matches_universe_window(
    signal_set: dict[str, Any],
    window_start: str,
    window_end: str,
    engine_ids: list[str] | None,
    asset_symbols: list[str] | None,
) -> bool:
    if engine_ids and signal_set["signal_engine_id"] not in set(engine_ids):
        return False
    if asset_symbols and signal_set["asset"] not in set(asset_symbols):
        return False
    start_ts = signal_set.get("coverage_start_ts") or signal_set.get("start_ts")
    end_ts = signal_set.get("coverage_end_ts") or signal_set.get("end_ts")
    if not start_ts or not end_ts:
        return True
    signal_start = _parse_datetime(start_ts)
    signal_end = _parse_datetime(end_ts)
    target_start = _parse_datetime(window_start)
    target_end = _parse_datetime(window_end)
    return signal_start <= target_end and signal_end >= target_start


def _parse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
