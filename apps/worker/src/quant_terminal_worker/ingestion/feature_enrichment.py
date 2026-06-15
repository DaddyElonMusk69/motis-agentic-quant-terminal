from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Protocol

from quant_terminal_api.repositories.market_data import PostgresMarketDataRepository
from quant_terminal_worker.ingestion.raw_candle_fill import _read_dataset_rows, _write_dataset_rows


TIMEFRAMES = ("5m", "2h", "1d")
EMA_PERIODS = (36, 43, 144, 169, 576, 676)


@dataclass(frozen=True, slots=True)
class FeatureFamily:
    key: str
    data_type: str
    label: str
    columns: tuple[str, ...]


FEATURE_FAMILIES: dict[str, FeatureFamily] = {
    "base_candle": FeatureFamily(
        key="base_candle",
        data_type="feature_base_candle",
        label="Base Candle Features",
        columns=("return_pct", "true_range_pct", "body_pct", "upper_wick_pct", "lower_wick_pct", "close_location_pct"),
    ),
    "volatility_range": FeatureFamily(
        key="volatility_range",
        data_type="feature_volatility_range",
        label="Volatility / Range",
        columns=("atr_14", "atr_pct_14", "rolling_range_pct_12", "rolling_range_pct_48", "range_compression_pct", "volatility_zscore_48"),
    ),
    "volume": FeatureFamily(
        key="volume",
        data_type="feature_volume",
        label="Volume",
        columns=("quote_volume_zscore_48", "quote_volume_ratio_48", "volume_expansion_flag"),
    ),
    "ema_vegas_structure": FeatureFamily(
        key="ema_vegas_structure",
        data_type="feature_ema_vegas_structure",
        label="EMA / Vegas Structure",
        columns=(
            *(f"ema_{period}_slope_pct" for period in EMA_PERIODS),
            "fast_tunnel_spread_pct",
            "mid_tunnel_spread_pct",
            "slow_tunnel_spread_pct",
            "fast_mid_gap_pct",
            "mid_slow_gap_pct",
            "ema_stack_state",
        ),
    ),
    "bollinger": FeatureFamily(
        key="bollinger",
        data_type="feature_bollinger",
        label="Bollinger Context",
        columns=("bb_mid_20", "bb_upper_20_2", "bb_lower_20_2", "bb_position_pct", "bb_bandwidth_pct", "bb_zscore"),
    ),
    "regime_momentum": FeatureFamily(
        key="regime_momentum",
        data_type="feature_regime_momentum",
        label="Regime / Momentum",
        columns=("return_pct_12", "return_pct_48", "range_position_pct_48", "trend_efficiency_48"),
    ),
}


class FeatureRefRepository(Protocol):
    def list_refs(self) -> list[dict[str, Any]]:
        ...

    def upsert_ref(self, registration: dict[str, Any]) -> None:
        ...


def enrich_feature_family_datasets(
    *,
    repository: FeatureRefRepository,
    asset: str,
    family: str,
    timeframes: tuple[str, ...] = TIMEFRAMES,
    start_date: str | date = "2025-01-01",
    target_root: Path = Path(".data/market-data"),
) -> dict[str, Any]:
    asset = asset.upper()
    family_spec = _feature_family(family)
    start = _coerce_date(start_date)
    source_refs = [
        ref
        for ref in repository.list_refs()
        if ref.get("asset") == asset
        and ref.get("data_type") == "candles"
        and ref.get("data_origin") == "derived"
        and ref.get("timeframe") in timeframes
    ]
    enriched: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for source_ref in source_refs:
        result = enrich_feature_family_dataset(
            source_registration=source_ref,
            repository=repository,
            family=family_spec.key,
            start_date=start,
            target_root=target_root,
        )
        if result["status"] == "enriched":
            enriched.append(result)
        else:
            skipped.append(result)
    return {
        "status": "enriched" if enriched else "noop",
        "asset": asset,
        "family": family_spec.key,
        "data_type": family_spec.data_type,
        "dataset_count": len(source_refs),
        "feature_count": len(enriched),
        "skipped_count": len(skipped),
        "features": enriched,
        "skipped": skipped,
    }


def enrich_feature_family_dataset(
    *,
    source_registration: dict[str, Any],
    repository: FeatureRefRepository,
    family: str,
    start_date: str | date,
    target_root: Path,
) -> dict[str, Any]:
    family_spec = _feature_family(family)
    rows = _read_dataset_rows(Path(source_registration["storage_uri"]))
    start = _coerce_date(start_date)
    rows = [row for row in rows if _coerce_datetime(row["timestamp"]).date() >= start]
    if not rows:
        return {
            "dataset_id": _feature_dataset_id(source_registration, family_spec),
            "status": "skipped",
            "reason": "empty_source_after_start_date",
        }
    feature_rows = build_feature_rows(rows, family=family_spec.key)
    storage_uri = (
        target_root
        / "origin=derived"
        / f"source={source_registration['source_id']}"
        / f"type={family_spec.data_type}"
        / f"asset={source_registration['asset']}"
        / f"timeframe={source_registration['timeframe']}"
    )
    _write_dataset_rows(storage_uri, feature_rows)
    registration = _feature_registration(
        source_registration=source_registration,
        family=family_spec,
        storage_uri=storage_uri,
        rows=feature_rows,
    )
    repository.upsert_ref(registration)
    return {
        "dataset_id": registration["dataset_id"],
        "status": "enriched",
        "asset": registration["asset"],
        "family": family_spec.key,
        "data_type": family_spec.data_type,
        "timeframe": registration["timeframe"],
        "row_count": len(feature_rows),
        "start_ts": feature_rows[0]["timestamp"],
        "end_ts": feature_rows[-1]["timestamp"],
        "columns": list(family_spec.columns),
    }


def build_feature_rows(rows: list[dict[str, Any]], *, family: str) -> list[dict[str, Any]]:
    family_spec = _feature_family(family)
    sorted_rows = sorted(rows, key=lambda row: _coerce_datetime(row["timestamp"]))
    enriched: list[dict[str, Any]] = []
    true_ranges: list[float] = []
    true_range_pcts: list[float | None] = []
    quote_volumes: list[float] = []
    for index, row in enumerate(sorted_rows):
        previous = sorted_rows[index - 1] if index > 0 else None
        true_range = _true_range(row, previous)
        true_ranges.append(true_range)
        close = _float(row["close"])
        previous_close = _float(previous["close"]) if previous else None
        true_range_pcts.append(_safe_pct(true_range, previous_close or close))
        quote_volumes.append(_quote_volume(row))
        feature_row = {
            "timestamp": row["timestamp"],
            **_family_values(
                rows=sorted_rows,
                index=index,
                true_ranges=true_ranges,
                true_range_pcts=true_range_pcts,
                quote_volumes=quote_volumes,
                family=family_spec.key,
            ),
        }
        enriched.append(feature_row)
    return enriched


def _family_values(
    *,
    rows: list[dict[str, Any]],
    index: int,
    true_ranges: list[float],
    true_range_pcts: list[float | None],
    quote_volumes: list[float],
    family: str,
) -> dict[str, Any]:
    row = rows[index]
    previous = rows[index - 1] if index > 0 else None
    if family == "base_candle":
        return _base_candle_values(row=row, previous=previous)
    if family == "volatility_range":
        return _volatility_values(rows=rows, index=index, true_ranges=true_ranges, true_range_pcts=true_range_pcts)
    if family == "volume":
        return _volume_values(quote_volumes=quote_volumes, index=index)
    if family == "ema_vegas_structure":
        return _ema_vegas_values(rows=rows, index=index)
    if family == "bollinger":
        return _bollinger_values(rows=rows, index=index)
    if family == "regime_momentum":
        return _regime_values(rows=rows, index=index)
    raise ValueError(f"Unsupported feature family: {family}")


def _base_candle_values(*, row: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, Any]:
    open_ = _float(row["open"])
    high = _float(row["high"])
    low = _float(row["low"])
    close = _float(row["close"])
    range_ = high - low
    previous_close = _float(previous["close"]) if previous else None
    return {
        "return_pct": _pct_change(previous_close, close),
        "true_range_pct": _safe_pct(_true_range(row, previous), previous_close or close),
        "body_pct": _safe_pct(abs(close - open_), range_),
        "upper_wick_pct": _safe_pct(high - max(open_, close), range_),
        "lower_wick_pct": _safe_pct(min(open_, close) - low, range_),
        "close_location_pct": _safe_pct(close - low, range_),
    }


def _volatility_values(
    *,
    rows: list[dict[str, Any]],
    index: int,
    true_ranges: list[float],
    true_range_pcts: list[float | None],
) -> dict[str, Any]:
    close = _float(rows[index]["close"])
    atr_14 = _rolling_mean(true_ranges, index, 14)
    range_12 = _rolling_range_pct(rows, index, 12)
    range_48 = _rolling_range_pct(rows, index, 48)
    return {
        "atr_14": atr_14,
        "atr_pct_14": _safe_pct(atr_14, close),
        "rolling_range_pct_12": range_12,
        "rolling_range_pct_48": range_48,
        "range_compression_pct": _safe_ratio_pct(range_12, range_48),
        "volatility_zscore_48": _zscore(true_range_pcts, index, 48),
    }


def _volume_values(*, quote_volumes: list[float], index: int) -> dict[str, Any]:
    current = quote_volumes[index]
    rolling = _rolling_values(quote_volumes, index, 48)
    avg = mean(rolling) if rolling else None
    zscore = _zscore(quote_volumes, index, 48)
    ratio = current / avg if avg and avg != 0 else None
    return {
        "quote_volume_zscore_48": zscore,
        "quote_volume_ratio_48": ratio,
        "volume_expansion_flag": bool(ratio is not None and ratio >= 1.5),
    }


def _ema_vegas_values(*, rows: list[dict[str, Any]], index: int) -> dict[str, Any]:
    row = rows[index]
    previous = rows[index - 1] if index > 0 else None
    close = _float(row["close"])
    values: dict[str, Any] = {}
    for period in EMA_PERIODS:
        key = f"ema_{period}"
        value = _optional_float(row.get(key))
        previous_value = _optional_float(previous.get(key)) if previous else None
        values[f"{key}_slope_pct"] = _pct_change(previous_value, value)
    fast = _avg_optional(_optional_float(row.get("ema_36")), _optional_float(row.get("ema_43")))
    mid = _avg_optional(_optional_float(row.get("ema_144")), _optional_float(row.get("ema_169")))
    slow = _avg_optional(_optional_float(row.get("ema_576")), _optional_float(row.get("ema_676")))
    values.update(
        {
            "fast_tunnel_spread_pct": _spread_pct(row, 36, 43, close),
            "mid_tunnel_spread_pct": _spread_pct(row, 144, 169, close),
            "slow_tunnel_spread_pct": _spread_pct(row, 576, 676, close),
            "fast_mid_gap_pct": _safe_pct((fast - mid) if fast is not None and mid is not None else None, close),
            "mid_slow_gap_pct": _safe_pct((mid - slow) if mid is not None and slow is not None else None, close),
            "ema_stack_state": _ema_stack_state(fast=fast, mid=mid, slow=slow),
        }
    )
    return values


def _bollinger_values(*, rows: list[dict[str, Any]], index: int) -> dict[str, Any]:
    closes = [_float(row["close"]) for row in _window(rows, index, 20)]
    close = _float(rows[index]["close"])
    if len(closes) < 2:
        mid = closes[-1] if closes else None
        std = None
    else:
        mid = mean(closes)
        std = pstdev(closes)
    upper = mid + 2 * std if mid is not None and std is not None else None
    lower = mid - 2 * std if mid is not None and std is not None else None
    band = upper - lower if upper is not None and lower is not None else None
    return {
        "bb_mid_20": mid,
        "bb_upper_20_2": upper,
        "bb_lower_20_2": lower,
        "bb_position_pct": _safe_pct((close - lower) if lower is not None else None, band),
        "bb_bandwidth_pct": _safe_pct(band, mid),
        "bb_zscore": ((close - mid) / std) if mid is not None and std not in (None, 0) else None,
    }


def _regime_values(*, rows: list[dict[str, Any]], index: int) -> dict[str, Any]:
    close = _float(rows[index]["close"])
    close_12 = _lag_close(rows, index, 12)
    close_48 = _lag_close(rows, index, 48)
    window = _window(rows, index, 48)
    lows = [_float(row["low"]) for row in window]
    highs = [_float(row["high"]) for row in window]
    path = sum(abs(_float(window[pos]["close"]) - _float(window[pos - 1]["close"])) for pos in range(1, len(window)))
    direct = abs(close - _float(window[0]["close"])) if window else None
    return {
        "return_pct_12": _pct_change(close_12, close),
        "return_pct_48": _pct_change(close_48, close),
        "range_position_pct_48": _safe_pct(close - min(lows), max(highs) - min(lows)) if lows and highs else None,
        "trend_efficiency_48": (direct / path) if direct is not None and path else None,
    }


def _feature_registration(
    *,
    source_registration: dict[str, Any],
    family: FeatureFamily,
    storage_uri: Path,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "dataset_id": _feature_dataset_id(source_registration, family),
        "source_id": source_registration["source_id"],
        "asset": source_registration["asset"],
        "instrument": source_registration["instrument"],
        "data_type": family.data_type,
        "timeframe": source_registration["timeframe"],
        "data_origin": "derived",
        "start_ts": rows[0]["timestamp"],
        "end_ts": rows[-1]["timestamp"],
        "row_count": len(rows),
        "storage_backend": "parquet",
        "storage_uri": str(storage_uri),
        "schema_descriptor": {
            "columns": ["timestamp", *family.columns],
            "feature_family": family.key,
            "label": family.label,
            "source_dataset_id": source_registration["dataset_id"],
            "source_data_type": source_registration["data_type"],
            "source_timeframe": source_registration["timeframe"],
        },
        "quality_status": "feature_enriched",
        "ingestion_version": "feature_enrichment.v1",
    }


def _feature_dataset_id(source_registration: dict[str, Any], family: FeatureFamily) -> str:
    return f"{source_registration['asset']}-{family.data_type}-{source_registration['timeframe']}"


def _feature_family(value: str) -> FeatureFamily:
    try:
        return FEATURE_FAMILIES[value]
    except KeyError as exc:
        raise ValueError(f"Unsupported feature family: {value}") from exc


def _window(rows: list[dict[str, Any]], index: int, size: int) -> list[dict[str, Any]]:
    return rows[max(0, index - size + 1) : index + 1]


def _rolling_values(values: list[float], index: int, size: int) -> list[float]:
    return values[max(0, index - size + 1) : index + 1]


def _rolling_mean(values: list[float], index: int, size: int) -> float | None:
    window = _rolling_values(values, index, size)
    return mean(window) if window else None


def _rolling_range_pct(rows: list[dict[str, Any]], index: int, size: int) -> float | None:
    window = _window(rows, index, size)
    if not window:
        return None
    high = max(_float(row["high"]) for row in window)
    low = min(_float(row["low"]) for row in window)
    close = _float(rows[index]["close"])
    return _safe_pct(high - low, close)


def _zscore(values: list[float | None], index: int, size: int) -> float | None:
    current = values[index]
    if current is None:
        return None
    window = [value for value in values[max(0, index - size + 1) : index + 1] if value is not None]
    if len(window) < 2:
        return None
    std = pstdev(window)
    return (current - mean(window)) / std if std else 0.0


def _true_range(row: dict[str, Any], previous: dict[str, Any] | None) -> float:
    high = _float(row["high"])
    low = _float(row["low"])
    previous_close = _float(previous["close"]) if previous else None
    if previous_close is None:
        return high - low
    return max(high - low, abs(high - previous_close), abs(low - previous_close))


def _quote_volume(row: dict[str, Any]) -> float:
    if row.get("vol_ccy_quote") not in (None, ""):
        return _float(row["vol_ccy_quote"])
    return _float(row.get("volume", 0)) * _float(row.get("close", 0))


def _spread_pct(row: dict[str, Any], left: int, right: int, close: float) -> float | None:
    left_value = _optional_float(row.get(f"ema_{left}"))
    right_value = _optional_float(row.get(f"ema_{right}"))
    if left_value is None or right_value is None:
        return None
    return _safe_pct(abs(left_value - right_value), close)


def _ema_stack_state(*, fast: float | None, mid: float | None, slow: float | None) -> str:
    if fast is None or mid is None or slow is None:
        return "unknown"
    if fast > mid > slow:
        return "bull_stack"
    if fast < mid < slow:
        return "bear_stack"
    return "mixed"


def _lag_close(rows: list[dict[str, Any]], index: int, lag: int) -> float | None:
    if index < lag:
        return None
    return _float(rows[index - lag]["close"])


def _avg_optional(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return (left + right) / 2


def _pct_change(start: float | None, end: float | None) -> float | None:
    if start in (None, 0) or end is None:
        return None
    return (end / start - 1) * 100


def _safe_pct(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator * 100


def _safe_ratio_pct(left: float | None, right: float | None) -> float | None:
    if left is None or right in (None, 0):
        return None
    return left / right * 100


def _float(value: Any) -> float:
    return float(Decimal(str(value)))


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return _float(value)


def _coerce_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build derived technical feature-family datasets from canonical derived candles.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--asset", required=True)
    parser.add_argument("--family", choices=sorted(FEATURE_FAMILIES), required=True)
    parser.add_argument("--start-date", default="2025-01-01")
    parser.add_argument("--target-root", default=Path(".data/market-data"), type=Path)
    args = parser.parse_args()
    result = enrich_feature_family_datasets(
        repository=PostgresMarketDataRepository(args.database_url),
        asset=args.asset,
        family=args.family,
        start_date=args.start_date,
        target_root=args.target_root,
    )
    print(result)


if __name__ == "__main__":
    main()
