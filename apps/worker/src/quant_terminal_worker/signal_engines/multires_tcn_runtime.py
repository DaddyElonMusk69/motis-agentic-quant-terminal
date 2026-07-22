from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import torch
from torch import nn

from quant_terminal_sdk.market_data_reader import MarketDataCandle


SEQUENCE_BINS = 64
BRANCH_CONFIG = {
    "5m": {"rule": None, "minutes": 5, "bars": 768, "bin_bars": 12},
    "15m": {"rule": "15min", "minutes": 15, "bars": 1344, "bin_bars": 21},
    "1h": {"rule": "1h", "minutes": 60, "bars": 1536, "bin_bars": 24},
    "4h": {"rule": "4h", "minutes": 240, "bars": 1536, "bin_bars": 24},
    "1d": {"rule": "1D", "minutes": 1440, "bars": 512, "bin_bars": 8},
}
RAW_CHANNELS = (
    "log_return",
    "open_gap",
    "high_excursion",
    "low_excursion",
    "range_log",
    "body_log",
    "upper_wick_fraction",
    "lower_wick_fraction",
    "close_location",
    "log_quote_volume_change",
    "quote_volume_zscore_60",
    "log_oi_change",
    "oi_level_zscore_60",
    "log_oi_notional_change",
    "oi_notional_zscore_60",
    "oi_range_log",
    "toptrader_position_log_ratio",
    "general_log_ratio",
    "taker_log_ratio",
    "toptrader_ratio_change",
    "general_ratio_change",
    "taker_ratio_change",
    "source_coverage",
)
BIN_STATS = ("mean", "std", "min", "max", "last")
BIN_CHANNEL_COUNT = len(RAW_CHANNELS) * len(BIN_STATS)


class SequenceStore:
    def __init__(
        self,
        *,
        available_ns: np.ndarray,
        binned_values: np.ndarray,
        bin_bars: int,
    ) -> None:
        self.available_ns = np.asarray(available_ns, dtype=np.int64)
        self.binned_values = np.asarray(binned_values, dtype=np.float32)
        self.bin_bars = int(bin_bars)
        self.minimum_index = self.bin_bars * SEQUENCE_BINS - 1
        self._offsets = self.bin_bars * np.arange(SEQUENCE_BINS - 1, -1, -1)

    def latest_index(self, decision_ns: int) -> int:
        return int(np.searchsorted(self.available_ns, decision_ns, side="right") - 1)

    def has_history(self, decision_ns: int) -> bool:
        return self.latest_index(decision_ns) >= self.minimum_index

    def tensor_at(self, decision_ns: int) -> np.ndarray | None:
        latest = self.latest_index(decision_ns)
        if latest < self.minimum_index:
            return None
        indices = latest - self._offsets
        return np.ascontiguousarray(self.binned_values[indices].T, dtype=np.float32)


class TemporalBranch(nn.Module):
    def __init__(self, input_channels: int) -> None:
        super().__init__()
        self.network = nn.Sequential(
            nn.BatchNorm1d(input_channels),
            nn.Conv1d(input_channels, 32, kernel_size=5, padding=2),
            nn.GELU(),
            nn.Conv1d(32, 32, kernel_size=3, padding=2, dilation=2),
            nn.GELU(),
            nn.Conv1d(32, 24, kernel_size=3, padding=1),
            nn.GELU(),
        )

    def forward(self, values: torch.Tensor) -> torch.Tensor:
        encoded = self.network(values)
        return torch.cat((encoded.mean(dim=-1), encoded.amax(dim=-1)), dim=1)


class MultiResolutionTCN(nn.Module):
    def __init__(self, input_channels: int = BIN_CHANNEL_COUNT) -> None:
        super().__init__()
        self.branches = nn.ModuleList(
            [TemporalBranch(input_channels) for _ in BRANCH_CONFIG]
        )
        self.fusion = nn.Sequential(
            nn.Linear(len(BRANCH_CONFIG) * 48, 96),
            nn.GELU(),
            nn.Dropout(0.10),
            nn.Linear(96, 48),
            nn.GELU(),
        )
        self.event_head = nn.Linear(48, 1)

    def encode(self, branches: list[torch.Tensor]) -> torch.Tensor:
        encoded = [branch(values) for branch, values in zip(self.branches, branches)]
        return self.fusion(torch.cat(encoded, dim=1))

    def forward(self, branches: list[torch.Tensor]) -> torch.Tensor:
        return self.event_head(self.encode(branches)).squeeze(1)


def align_market_rows(
    *,
    raw_5m: list[MarketDataCandle],
    raw_oi: list[dict[str, Any]],
) -> pd.DataFrame:
    candles = {
        _timestamp(candle.timestamp): {
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
            "volume": float(candle.volume),
            "vol_ccy_quote": float(candle.vol_ccy_quote),
        }
        for candle in raw_5m
        if int(candle.confirm) == 1
    }
    metrics = {
        _timestamp(row.get("timestamp") or row.get("ts")): {
            "sum_open_interest": _float(row.get("sum_open_interest")),
            "sum_open_interest_value": _float(row.get("sum_open_interest_value")),
            "sum_toptrader_long_short_ratio": _float(
                row.get("top_trader_position_long_short_ratio")
            ),
            "count_long_short_ratio": _float(
                row.get("global_account_long_short_ratio")
            ),
            "sum_taker_long_short_vol_ratio": _float(
                row.get("taker_buy_sell_volume_ratio")
            ),
        }
        for row in raw_oi
        if int(row.get("confirm", 1)) == 1 and bool(row.get("complete", True))
    }
    timestamps = sorted(set(candles) & set(metrics))
    if not timestamps:
        return pd.DataFrame()
    frame = pd.DataFrame(
        [{**candles[timestamp], **metrics[timestamp]} for timestamp in timestamps],
        index=pd.DatetimeIndex(timestamps, name="timestamp"),
    )
    frame["source_coverage"] = 1.0
    return frame


def build_sequence_stores(source: pd.DataFrame) -> dict[str, SequenceStore]:
    stores = {}
    for timeframe, config in BRANCH_CONFIG.items():
        aggregated = aggregate_resolution(
            source,
            rule=config["rule"],
            expected_rows=max(1, config["minutes"] // 5),
        )
        channels = build_raw_channels(aggregated)
        binned = build_binned_channels(channels, bin_bars=config["bin_bars"])
        available = (
            aggregated.index + pd.Timedelta(minutes=config["minutes"])
        ).astype("int64").to_numpy()
        stores[timeframe] = SequenceStore(
            available_ns=available,
            binned_values=binned,
            bin_bars=config["bin_bars"],
        )
    return stores


def ready_decision_indices(
    *,
    source: pd.DataFrame,
    stores: dict[str, SequenceStore],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> np.ndarray:
    source_ns = source.index.astype("int64").to_numpy(dtype=np.int64)
    decision_ns = source_ns + 5 * 60 * 1_000_000_000
    range_mask = (source.index >= start) & (source.index <= end)
    return np.asarray(
        [
            index
            for index in np.flatnonzero(range_mask)
            if all(store.has_history(int(decision_ns[index])) for store in stores.values())
        ],
        dtype=int,
    )


def score_decisions(
    *,
    model: MultiResolutionTCN,
    decision_ns: np.ndarray,
    stores: dict[str, SequenceStore],
    batch_size: int = 512,
) -> np.ndarray:
    model.eval()
    output = np.empty(len(decision_ns), dtype=np.float32)
    with torch.inference_mode():
        for start in range(0, len(decision_ns), batch_size):
            timestamps = decision_ns[start : start + batch_size]
            branches = [
                torch.from_numpy(
                    np.stack([stores[name].tensor_at(int(value)) for value in timestamps])
                )
                for name in BRANCH_CONFIG
            ]
            scores = torch.sigmoid(model(branches)).cpu().numpy().astype(np.float32)
            output[start : start + len(scores)] = scores
    return output


@lru_cache(maxsize=4)
def load_model_artifact(path: Path) -> tuple[MultiResolutionTCN, dict[str, Any]]:
    artifact = torch.load(path, map_location="cpu", weights_only=True)
    if artifact.get("schema_version") != "motis_multires_tcn_model.v1":
        raise ValueError("unsupported multi-resolution model artifact")
    if artifact.get("branch_config") != BRANCH_CONFIG:
        raise ValueError("model branch configuration does not match runtime")
    if tuple(artifact.get("raw_channels") or ()) != RAW_CHANNELS:
        raise ValueError("model raw channels do not match runtime")
    model = MultiResolutionTCN()
    model.load_state_dict(artifact["state_dict"])
    model.eval()
    return model, artifact


def aggregate_resolution(
    source: pd.DataFrame,
    *,
    rule: str | None,
    expected_rows: int,
) -> pd.DataFrame:
    columns = [
        "open",
        "high",
        "low",
        "close",
        "vol_ccy_quote",
        "volume",
        "sum_open_interest",
        "sum_open_interest_value",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    ]
    frame = source[columns].copy()
    if rule is None:
        result = frame
        result["oi_first"] = result["sum_open_interest"]
        result["oi_min"] = result["sum_open_interest"]
        result["oi_max"] = result["sum_open_interest"]
        result["source_coverage"] = 1.0
        return result
    result = frame.resample(rule, origin="epoch", label="left", closed="left").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "vol_ccy_quote": "sum",
            "volume": "sum",
            "sum_open_interest": ["first", "last", "min", "max"],
            "sum_open_interest_value": "last",
            "sum_toptrader_long_short_ratio": "last",
            "count_long_short_ratio": "last",
            "sum_taker_long_short_vol_ratio": "last",
        }
    )
    result.columns = [
        "open",
        "high",
        "low",
        "close",
        "vol_ccy_quote",
        "volume",
        "oi_first",
        "sum_open_interest",
        "oi_min",
        "oi_max",
        "sum_open_interest_value",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    ]
    count = frame["close"].resample(
        rule, origin="epoch", label="left", closed="left"
    ).count()
    result["source_coverage"] = (count / expected_rows).clip(upper=1.0)
    return result


def build_raw_channels(frame: pd.DataFrame) -> pd.DataFrame:
    close = frame["close"].astype(float)
    previous_close = close.shift(1)
    open_ = frame["open"].astype(float)
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    candle_range = (high - low).replace(0, np.nan)
    quote_volume = frame["vol_ccy_quote"].astype(float).where(
        frame["vol_ccy_quote"] > 0,
        frame["volume"].astype(float),
    )
    oi = frame["sum_open_interest"].astype(float)
    oi_value = frame["sum_open_interest_value"].astype(float)
    channels = pd.DataFrame(index=frame.index)
    channels["log_return"] = _safe_log_ratio(close, previous_close)
    channels["open_gap"] = _safe_log_ratio(open_, previous_close)
    channels["high_excursion"] = _safe_log_ratio(high, previous_close)
    channels["low_excursion"] = _safe_log_ratio(low, previous_close)
    channels["range_log"] = _safe_log_ratio(high, low)
    channels["body_log"] = _safe_log_ratio(close, open_)
    channels["upper_wick_fraction"] = (
        high - pd.concat([open_, close], axis=1).max(axis=1)
    ) / candle_range
    channels["lower_wick_fraction"] = (
        pd.concat([open_, close], axis=1).min(axis=1) - low
    ) / candle_range
    channels["close_location"] = (close - low) / candle_range
    log_volume = np.log1p(quote_volume.clip(lower=0))
    channels["log_quote_volume_change"] = log_volume.diff()
    channels["quote_volume_zscore_60"] = rolling_zscore(log_volume, 60)
    log_oi = np.log(oi.where(oi > 0))
    channels["log_oi_change"] = log_oi.diff()
    channels["oi_level_zscore_60"] = rolling_zscore(log_oi, 60)
    log_oi_value = np.log(oi_value.where(oi_value > 0))
    channels["log_oi_notional_change"] = log_oi_value.diff()
    channels["oi_notional_zscore_60"] = rolling_zscore(log_oi_value, 60)
    channels["oi_range_log"] = _safe_log_ratio(frame["oi_max"], frame["oi_min"])
    ratio_sources = {
        "toptrader": frame["sum_toptrader_long_short_ratio"].astype(float).round(4),
        "general": frame["count_long_short_ratio"].astype(float).round(4),
        "taker": frame["sum_taker_long_short_vol_ratio"].astype(float).round(4),
    }
    for name, values in ratio_sources.items():
        log_ratio = np.log(values.where(values > 0))
        output = "toptrader_position_log_ratio" if name == "toptrader" else f"{name}_log_ratio"
        channels[output] = log_ratio
        channels[f"{name}_ratio_change"] = log_ratio.diff()
    channels["source_coverage"] = frame["source_coverage"].astype(float)
    return channels[list(RAW_CHANNELS)].replace([np.inf, -np.inf], np.nan)


def rolling_zscore(values: pd.Series, window: int) -> pd.Series:
    minimum = max(8, window // 3)
    mean = values.rolling(window, min_periods=minimum).mean()
    std = values.rolling(window, min_periods=minimum).std(ddof=1)
    return (values - mean) / std.replace(0, np.nan)


def build_binned_channels(channels: pd.DataFrame, *, bin_bars: int) -> np.ndarray:
    rolling = channels.rolling(bin_bars, min_periods=bin_bars)
    parts = [
        rolling.mean(),
        rolling.std(ddof=1),
        rolling.min(),
        rolling.max(),
        channels,
    ]
    output = np.concatenate(
        [part.to_numpy(dtype=np.float32, na_value=np.nan) for part in parts],
        axis=1,
    )
    return np.nan_to_num(output, nan=0.0, posinf=0.0, neginf=0.0)


def _safe_log_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    valid = (numerator > 0) & (denominator > 0)
    result = pd.Series(np.nan, index=numerator.index, dtype=float)
    result.loc[valid] = np.log(numerator.loc[valid] / denominator.loc[valid])
    return result


def _timestamp(value: Any) -> pd.Timestamp:
    return pd.Timestamp(value).tz_convert("UTC") if pd.Timestamp(value).tz else pd.Timestamp(value).tz_localize("UTC")


def _float(value: Any) -> float:
    return float(value) if value not in (None, "") else float("nan")
