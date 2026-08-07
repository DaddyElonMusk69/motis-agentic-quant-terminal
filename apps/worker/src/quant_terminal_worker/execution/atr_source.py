from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

from quant_terminal_sdk.market_data_reader import read_rows_from_ref


ATR_DATA_TYPE = "technical_indicator_atr"
DEFAULT_VALUE_FIELD = "atr_pct"
DEFAULT_LOOKUP = "latest_available_at_or_before_entry"


class ATRSourceError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ResolvedATRPolicy:
    policy: dict[str, Any]
    diagnostics: dict[str, Any]


def has_atr_source(policy: Mapping[str, Any], fallback: Mapping[str, Any] | None = None) -> bool:
    source = _atr_source(policy, fallback=fallback)
    return source is not None and _truthy(source.get("enabled", True))


def resolve_atr_policy(
    policy: Mapping[str, Any],
    *,
    fallback: Mapping[str, Any] | None = None,
    signal_timestamp: str | datetime | None,
    market_data_repository: Any | None,
    workspace_root: str | Path,
    asset: str | None = None,
) -> ResolvedATRPolicy:
    source = _normalize_source(_atr_source(policy, fallback=fallback))
    if source is None:
        return ResolvedATRPolicy(policy=dict(policy), diagnostics={})
    if market_data_repository is None:
        raise ATRSourceError("atr_source requires market_data_repository")

    row = _resolve_row(
        source=source,
        signal_timestamp=_coerce_datetime(signal_timestamp),
        market_data_repository=market_data_repository,
        workspace_root=Path(workspace_root),
        asset=asset,
    )
    base_pct = _positive_float(row.get(source["value_field"]))
    if base_pct <= 0:
        raise ATRSourceError(f"atr_source row has no positive {source['value_field']}")

    resolved = dict(policy)
    final_tp_pct = round(base_pct * source["tp_multiplier"], 8)
    initial_sl_pct = round(base_pct * source["sl_multiplier"], 8)
    resolved.update(
        {
            "tp_pct": final_tp_pct,
            "sl_pct": initial_sl_pct,
            "final_tp_pct": final_tp_pct,
            "lock_profit_pct": final_tp_pct,
            "initial_sl_pct": initial_sl_pct,
            "exit_policy_type": "atr_dynamic",
        }
    )
    if bool(resolved.get("protection_enabled")):
        protect_multiplier = source.get("protect_trigger_multiplier")
        trail_multiplier = source.get("trail_sl_multiplier")
        if protect_multiplier is not None:
            resolved["protect_trigger_pct"] = round(base_pct * float(protect_multiplier), 8)
        if trail_multiplier is not None:
            resolved["trail_sl_pct"] = round(base_pct * float(trail_multiplier), 8)

    diagnostics = {
        "atr_source": {
            "enabled": True,
            "dataset_id": row.get("_dataset_id") or source["dataset_id"],
            "timeframe": source.get("timeframe"),
            "period": source.get("period"),
            "value_field": source["value_field"],
            "lookup": source["lookup"],
            "base_atr_pct": round(base_pct, 8),
            "atr_timestamp": _iso(row.get("timestamp")),
            "atr_available_at": _iso(row.get("available_at")),
            "tp_multiplier": source["tp_multiplier"],
            "sl_multiplier": source["sl_multiplier"],
            "final_tp_pct": final_tp_pct,
            "initial_sl_pct": initial_sl_pct,
        }
    }
    if bool(resolved.get("protection_enabled")):
        diagnostics["atr_source"].update(
            {
                "protect_trigger_multiplier": source.get("protect_trigger_multiplier"),
                "trail_sl_multiplier": source.get("trail_sl_multiplier"),
                "protect_trigger_pct": resolved.get("protect_trigger_pct"),
                "trail_sl_pct": resolved.get("trail_sl_pct"),
            }
        )
    return ResolvedATRPolicy(policy=resolved, diagnostics=diagnostics)


def _atr_source(policy: Mapping[str, Any], fallback: Mapping[str, Any] | None = None) -> Mapping[str, Any] | None:
    source = policy.get("atr_source")
    if isinstance(source, Mapping):
        return source
    fallback_source = fallback.get("atr_source") if fallback is not None else None
    return fallback_source if isinstance(fallback_source, Mapping) else None


def _normalize_source(source: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if source is None or not _truthy(source.get("enabled", True)):
        return None
    dataset_id = str(source.get("dataset_id") or "").strip()
    value_field = str(source.get("value_field") or DEFAULT_VALUE_FIELD).strip()
    if value_field != DEFAULT_VALUE_FIELD:
        raise ATRSourceError("atr_source currently supports value_field='atr_pct' only")
    tp_multiplier = _positive_float(source.get("tp_multiplier"))
    sl_multiplier = _positive_float(source.get("sl_multiplier"))
    if tp_multiplier <= 0:
        raise ATRSourceError("atr_source requires positive tp_multiplier")
    if sl_multiplier <= 0:
        raise ATRSourceError("atr_source requires positive sl_multiplier")
    normalized = {
        "dataset_id": dataset_id or None,
        "asset": str(source.get("asset") or "").strip().upper() or None,
        "data_type": str(source.get("data_type") or ATR_DATA_TYPE),
        "origin": str(source.get("origin") or "derived"),
        "timeframe": str(source.get("timeframe") or ""),
        "period": source.get("period"),
        "value_field": value_field,
        "lookup": str(source.get("lookup") or DEFAULT_LOOKUP),
        "tp_multiplier": tp_multiplier,
        "sl_multiplier": sl_multiplier,
        "protect_trigger_multiplier": _optional_positive_float(source.get("protect_trigger_multiplier")),
        "trail_sl_multiplier": _optional_positive_float(source.get("trail_sl_multiplier")),
    }
    if normalized["data_type"] != ATR_DATA_TYPE:
        raise ATRSourceError("atr_source data_type must be technical_indicator_atr")
    if normalized["lookup"] != DEFAULT_LOOKUP:
        raise ATRSourceError(f"atr_source lookup must be {DEFAULT_LOOKUP}")
    return normalized


def _resolve_row(
    *,
    source: Mapping[str, Any],
    signal_timestamp: datetime,
    market_data_repository: Any,
    workspace_root: Path,
    asset: str | None,
) -> dict[str, Any]:
    ref_getter = getattr(market_data_repository, "get_ref", None)
    if not callable(ref_getter):
        raise ATRSourceError("market_data_repository cannot resolve atr_source dataset_id")
    ref = None
    if source.get("dataset_id"):
        ref = ref_getter(str(source["dataset_id"]))
    else:
        asset_key = str(asset or source.get("asset") or "").strip().upper()
        timeframe = str(source.get("timeframe") or "").strip()
        if not asset_key or not timeframe:
            raise ATRSourceError("atr_source requires dataset_id or asset/timeframe")
        ref_getter = getattr(market_data_repository, "get_data_ref", None)
        if not callable(ref_getter):
            raise ATRSourceError("market_data_repository cannot resolve atr_source by asset/timeframe")
        ref = ref_getter(
            asset=asset_key,
            timeframe=timeframe,
            origin=str(source.get("origin") or "derived"),
            data_type=ATR_DATA_TYPE,
        )
    if ref is None:
        dataset_id = source.get("dataset_id") or f"{asset or source.get('asset') or 'unknown'}:{source.get('timeframe')}"
        raise ATRSourceError(f"atr_source dataset not found: {dataset_id}")
    if ref.get("data_type") != ATR_DATA_TYPE:
        raise ATRSourceError(f"atr_source dataset is not {ATR_DATA_TYPE}: {ref.get('dataset_id')}")
    series = _cached_atr_series(
        dataset_id=str(ref["dataset_id"]),
        storage_uri=str(ref["storage_uri"]),
        storage_backend=str(ref.get("storage_backend") or "parquet"),
        workspace_root=str(Path(workspace_root).resolve()),
        ref_end=str(ref.get("end_ts") or ""),
        ref_row_count=str(ref.get("row_count") or ""),
    )
    available_at = [row["available_at"] for row in series]
    index = bisect_right(available_at, signal_timestamp) - 1
    while index >= 0:
        row = series[index]
        if row.get("warmup_complete", True) and row.get(source["value_field"]) is not None:
            resolved = dict(row)
            resolved["_dataset_id"] = ref.get("dataset_id")
            return resolved
        index -= 1
    raise ATRSourceError(f"atr_source has no warm ATR row available at or before {signal_timestamp.isoformat()}")


@lru_cache(maxsize=64)
def _cached_atr_series(
    *,
    dataset_id: str,
    storage_uri: str,
    storage_backend: str,
    workspace_root: str,
    ref_end: str,
    ref_row_count: str,
) -> tuple[dict[str, Any], ...]:
    del ref_end, ref_row_count
    rows = read_rows_from_ref(
        {
            "dataset_id": dataset_id,
            "storage_backend": storage_backend,
            "storage_uri": storage_uri,
        },
        workspace_root=workspace_root,
        confirmed_only=True,
    )
    normalized = []
    for row in rows:
        if row.get("atr_pct") is None:
            continue
        available_at = _coerce_datetime(row.get("available_at") or row.get("interval_end") or row.get("timestamp"))
        normalized.append(
            {
                **row,
                "timestamp": _coerce_datetime(row.get("timestamp")),
                "available_at": available_at,
                "atr": _optional_float(row.get("atr")),
                "atr_pct": _optional_float(row.get("atr_pct")),
                "warmup_complete": _truthy(row.get("warmup_complete", True)),
            }
        )
    normalized.sort(key=lambda row: row["available_at"])
    return tuple(normalized)


def _coerce_datetime(value: str | datetime | None) -> datetime:
    if value is None:
        raise ATRSourceError("atr_source lookup timestamp is missing")
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _positive_float(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if parsed > 0 else 0.0


def _optional_positive_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    parsed = _positive_float(value)
    if parsed <= 0:
        raise ATRSourceError("atr_source multiplier values must be positive")
    return parsed


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _iso(value: Any) -> str | None:
    if value in (None, ""):
        return None
    timestamp = _coerce_datetime(value)
    return timestamp.isoformat().replace("+00:00", "Z")
