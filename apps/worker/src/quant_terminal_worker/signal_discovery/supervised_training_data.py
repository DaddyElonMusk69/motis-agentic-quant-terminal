from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import torch
from torch.utils.data import Dataset


SOURCE_INTERVAL = pd.Timedelta(minutes=5)
FEATURE_SCHEMA_VERSION = "crypto_perpetual_dense_multires_v1"
MODULE_IMPORT = (
    "quant_terminal_worker.signal_discovery.supervised_training_data"
    ":TimestampTrainingDataModule"
)
SOURCE_SELECTORS = {
    "candles": {"data_type": "candles", "data_origin": "raw", "timeframe": "5m"},
    "open_interest": {
        "data_type": "open_interest",
        "data_origin": "raw",
        "timeframe": "5m",
    },
    "futures_metrics": {
        "data_type": "futures_metrics",
        "data_origin": "raw",
        "timeframe": "5m",
    },
    "premium_index": {
        "data_type": "premium_index",
        "data_origin": "raw",
        "timeframe": "5m",
    },
    "funding_features": {
        "data_type": "funding_features",
        "data_origin": "derived",
        "timeframe": "5m",
    },
    "funding_events": {
        "data_type": "funding",
        "data_origin": "raw",
        "timeframe": "8h",
    },
}


@dataclass(frozen=True)
class BranchSpec:
    name: str
    rule: str | None
    interval_minutes: int
    steps: int
    source: str = "market"

    @property
    def lookback_days(self) -> float:
        return self.interval_minutes * self.steps / 1440.0


BRANCH_SPECS = (
    BranchSpec("5m_micro", None, 5, 2016),
    BranchSpec("15m_short", "15min", 15, 2880),
    BranchSpec("1h_medium", "1h", 60, 2160),
    BranchSpec("4h_long", "4h", 240, 2190),
    BranchSpec("1d_regime", "1D", 1440, 384),
    BranchSpec("funding_events", None, 480, 1095, source="funding_events"),
)


@dataclass(frozen=True)
class FrozenLabelTarget:
    risk_pct: float = 1.0
    reward_multiple: float = 2.0
    stop_multiple: float = 1.0
    entry_delay_minutes: int = 5
    horizon_hours: float = 48.0


@dataclass(frozen=True)
class TrainingDataConfig:
    branch_specs: tuple[BranchSpec, ...] = BRANCH_SPECS
    target: FrozenLabelTarget = FrozenLabelTarget()
    clip_value: float = 8.0
    research_start: pd.Timestamp = pd.Timestamp("1970-01-01T00:00:00Z")
    research_end: pd.Timestamp = pd.Timestamp("1970-01-01T00:00:00Z")
    walk_forward_start: pd.Timestamp = pd.Timestamp("1970-01-01T00:00:00Z")


@dataclass(frozen=True)
class FeatureTimeline:
    name: str
    available_ns: np.ndarray
    values: np.ndarray
    channel_names: tuple[str, ...]
    spec: BranchSpec

    def __post_init__(self) -> None:
        if self.values.ndim != 2:
            raise ValueError(f"{self.name} values must be rows by channels")
        if len(self.available_ns) != len(self.values):
            raise ValueError(f"{self.name} availability and value rows differ")
        if self.values.shape[1] != len(self.channel_names):
            raise ValueError(f"{self.name} channel names do not match values")
        if len(self.available_ns) and np.any(np.diff(self.available_ns) <= 0):
            raise ValueError(f"{self.name} availability must be strictly increasing")


@dataclass(frozen=True)
class BranchPreprocessorState:
    channel_names: tuple[str, ...]
    center: np.ndarray
    scale: np.ndarray
    clip_value: float
    fitted_through_ns: int


@dataclass(frozen=True)
class PreprocessorState:
    schema_version: str
    branches: dict[str, BranchPreprocessorState]


@dataclass(frozen=True)
class TransformedTimeline:
    name: str
    available_ns: np.ndarray
    values: np.ndarray
    channel_names: tuple[str, ...]
    spec: BranchSpec

    def latest_index(self, decision_ns: int) -> int:
        return int(np.searchsorted(self.available_ns, decision_ns, side="right") - 1)

    def has_history(self, decision_ns: int) -> bool:
        return self.latest_index(decision_ns) >= self.spec.steps - 1

    def sequence_at(self, decision_ns: int) -> np.ndarray:
        latest = self.latest_index(decision_ns)
        first = latest - self.spec.steps + 1
        if first < 0:
            raise ValueError(f"{self.name} lacks complete history at {decision_ns}")
        sequence = self.values[first : latest + 1]
        return np.ascontiguousarray(sequence.T, dtype=np.float32)


class TimestampDataset(Dataset[dict[str, Any]]):
    def __init__(
        self,
        *,
        sample_index: pd.DataFrame,
        timelines: Mapping[str, TransformedTimeline],
    ) -> None:
        self.sample_index = sample_index.reset_index(drop=True)
        self.timelines = dict(timelines)

    def __len__(self) -> int:
        return len(self.sample_index)

    def __getitem__(self, index: int) -> dict[str, Any]:
        row = self.sample_index.iloc[index]
        decision_ns = int(row["decision_ns"])
        return {
            "decision_ns": torch.tensor(decision_ns, dtype=torch.int64),
            "target": torch.tensor(float(row["target"]), dtype=torch.float32),
            "sample_weight": torch.tensor(1.0, dtype=torch.float32),
            "branches": {
                name: torch.from_numpy(timeline.sequence_at(decision_ns))
                for name, timeline in self.timelines.items()
            },
        }


class TimestampTrainingDataModule:
    def __init__(
        self,
        *,
        workspace_root: Path,
        artifact_root: Path,
        session_id: str,
        target_config_hash: str,
        dataset_ids: Mapping[str, str],
        config: TrainingDataConfig,
    ) -> None:
        self.workspace_root = workspace_root.resolve()
        self.artifact_root = artifact_root.resolve()
        self.session_id = session_id
        self.target_config_hash = target_config_hash
        self.dataset_ids = dict(dataset_ids)
        self.config = config
        self.manifest_path = self.artifact_root / "evidence" / "evidence_manifest.json"
        self.label_path = self.artifact_root / "atlas" / "training_timestamp_labels.parquet"

    @classmethod
    def from_session(
        cls,
        *,
        workspace_root: Path,
        artifact_root: Path,
        branch_specs: tuple[BranchSpec, ...] = BRANCH_SPECS,
        clip_value: float = 8.0,
    ) -> TimestampTrainingDataModule:
        root = artifact_root.resolve()
        frozen = json.loads((root / "target" / "frozen_target.json").read_text())
        manifest = json.loads((root / "evidence" / "evidence_manifest.json").read_text())
        selected_target = frozen["selected_target"]
        splits = frozen["splits"]
        source_dataset_id = str((frozen.get("source_data") or {}).get("dataset_id") or "")
        dataset_ids = select_training_datasets(
            manifest,
            preferred_candle_dataset_id=source_dataset_id or None,
        )
        return cls(
            workspace_root=workspace_root,
            artifact_root=root,
            session_id=str(frozen["session_id"]),
            target_config_hash=str(frozen["config_hash"]),
            dataset_ids=dataset_ids,
            config=TrainingDataConfig(
                branch_specs=branch_specs,
                target=FrozenLabelTarget(
                    risk_pct=float(selected_target["selected_risk_pct"]),
                    reward_multiple=float(selected_target["reward_multiple"]),
                    stop_multiple=float(selected_target["stop_multiple"]),
                    entry_delay_minutes=int(selected_target["entry_delay_minutes"]),
                    horizon_hours=float(selected_target["horizon_hours"]),
                ),
                clip_value=float(clip_value),
                research_start=pd.Timestamp(splits["research_start"]),
                research_end=pd.Timestamp(splits["research_end"]),
                walk_forward_start=pd.Timestamp(splits["walk_forward_start"]),
            ),
        )

    def load_sources(self) -> dict[str, pd.DataFrame]:
        manifest = json.loads(self.manifest_path.read_text())
        return {
            role: read_manifest_dataset(
                root=self.workspace_root,
                manifest=manifest,
                dataset_id=dataset_id,
                authorized_end=self.config.research_end,
            )
            for role, dataset_id in self.dataset_ids.items()
        }

    def load_labels(self) -> pd.DataFrame:
        return load_raw_timestamp_labels(
            self.label_path,
            target=self.config.target,
            research_start=self.config.research_start,
            research_end=self.config.research_end,
            walk_forward_start=self.config.walk_forward_start,
        )

    def build_feature_timelines(
        self,
        sources: Mapping[str, pd.DataFrame] | None = None,
    ) -> dict[str, FeatureTimeline]:
        loaded = dict(sources) if sources is not None else self.load_sources()
        market = build_aligned_market_frame(
            candles=loaded["candles"],
            open_interest=loaded["open_interest"],
            futures_metrics=loaded["futures_metrics"],
            premium_index=loaded["premium_index"],
            funding_features=loaded["funding_features"],
        )
        timelines: dict[str, FeatureTimeline] = {}
        for spec in self.config.branch_specs:
            if spec.source != "market":
                continue
            aggregated = aggregate_market_resolution(
                market,
                rule=spec.rule,
                expected_rows=max(1, spec.interval_minutes // 5),
            )
            features = build_market_features(aggregated)
            timelines[spec.name] = FeatureTimeline(
                name=spec.name,
                available_ns=(
                    aggregated.index + pd.Timedelta(minutes=spec.interval_minutes)
                ).astype("int64").to_numpy(dtype=np.int64),
                values=features.to_numpy(dtype=np.float32, na_value=np.nan),
                channel_names=tuple(features.columns),
                spec=spec,
            )
        funding_spec = next(
            spec for spec in self.config.branch_specs if spec.source == "funding_events"
        )
        funding_features, funding_available_ns = build_funding_event_features(
            loaded["funding_events"]
        )
        timelines[funding_spec.name] = FeatureTimeline(
            name=funding_spec.name,
            available_ns=funding_available_ns,
            values=funding_features.to_numpy(dtype=np.float32, na_value=np.nan),
            channel_names=tuple(funding_features.columns),
            spec=funding_spec,
        )
        return timelines

    def fit_preprocessor(
        self,
        timelines: Mapping[str, FeatureTimeline],
        *,
        train_end: pd.Timestamp,
    ) -> PreprocessorState:
        return fit_preprocessor(
            timelines,
            train_end=train_end,
            clip_value=self.config.clip_value,
        )

    def transform_timelines(
        self,
        timelines: Mapping[str, FeatureTimeline],
        preprocessor: PreprocessorState,
    ) -> dict[str, TransformedTimeline]:
        return transform_timelines(timelines, preprocessor)

    def build_sample_index(
        self,
        labels: pd.DataFrame,
        timelines: Mapping[str, FeatureTimeline | TransformedTimeline],
        *,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        return build_sample_index(labels, timelines, start=start, end=end)

    def dataset(
        self,
        *,
        sample_index: pd.DataFrame,
        timelines: Mapping[str, TransformedTimeline],
    ) -> TimestampDataset:
        return TimestampDataset(sample_index=sample_index, timelines=timelines)

    def audit(
        self,
        *,
        labels: pd.DataFrame,
        timelines: Mapping[str, FeatureTimeline],
        preprocessor: PreprocessorState | None = None,
    ) -> dict[str, Any]:
        return build_data_audit(
            labels=labels,
            timelines=timelines,
            preprocessor=preprocessor,
        )


def select_training_datasets(
    manifest: Mapping[str, Any],
    *,
    preferred_candle_dataset_id: str | None = None,
) -> dict[str, str]:
    entries = list(manifest.get("included_datasets") or ())
    selected: dict[str, str] = {}
    for role, selector in SOURCE_SELECTORS.items():
        candidates = [
            entry
            for entry in entries
            if all(entry.get(key) == value for key, value in selector.items())
        ]
        if role == "candles" and preferred_candle_dataset_id:
            preferred = [
                entry
                for entry in candidates
                if entry.get("dataset_id") == preferred_candle_dataset_id
            ]
            if preferred:
                candidates = preferred
        if len(candidates) != 1:
            ids = sorted(str(entry.get("dataset_id")) for entry in candidates)
            raise ValueError(
                f"expected one {role} dataset matching {selector}, found {ids}"
            )
        selected[role] = str(candidates[0]["dataset_id"])
    return selected


def read_manifest_dataset(
    *,
    root: Path,
    manifest: dict[str, Any],
    dataset_id: str,
    authorized_end: pd.Timestamp,
) -> pd.DataFrame:
    entry = next(
        item for item in manifest["included_datasets"] if item["dataset_id"] == dataset_id
    )
    if pd.Timestamp(entry["authorized_end"]) != authorized_end:
        raise ValueError(f"authorization cutoff changed for {dataset_id}")
    columns = list(entry["schema_columns"])
    frames = [
        pq.read_table(root / shard["path"], columns=columns).to_pandas()
        for shard in entry["parquet_shards"]
    ]
    frame = pd.concat(frames, ignore_index=True)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    return (
        frame[frame["timestamp"] <= authorized_end]
        .sort_values("timestamp")
        .drop_duplicates("timestamp", keep="last")
        .reset_index(drop=True)
    )


def load_raw_timestamp_labels(
    path: Path,
    *,
    target: FrozenLabelTarget,
    research_start: pd.Timestamp,
    research_end: pd.Timestamp,
    walk_forward_start: pd.Timestamp,
) -> pd.DataFrame:
    columns = [
        "decision_ts",
        "horizon_end_ts",
        "risk_pct",
        "reward_multiple",
        "stop_multiple",
        "scenario_entry_delay_minutes",
        "scenario_horizon_hours",
        "label",
    ]
    labels = pq.read_table(path, columns=columns).to_pandas()
    labels["decision_ts"] = pd.to_datetime(labels["decision_ts"], utc=True)
    labels["horizon_end_ts"] = pd.to_datetime(labels["horizon_end_ts"], utc=True)
    selected = (
        np.isclose(labels["risk_pct"], target.risk_pct)
        & np.isclose(labels["reward_multiple"], target.reward_multiple)
        & np.isclose(labels["stop_multiple"], target.stop_multiple)
        & (labels["scenario_entry_delay_minutes"] == target.entry_delay_minutes)
        & np.isclose(labels["scenario_horizon_hours"], target.horizon_hours)
        & (labels["decision_ts"] >= research_start)
        & (labels["decision_ts"] <= research_end)
        & (labels["horizon_end_ts"] < walk_forward_start)
    )
    labels = labels.loc[selected].copy()
    unknown = sorted(set(labels["label"].dropna().unique()) - {"LONG", "SHORT", "NEUTRAL", "AMBIGUOUS"})
    if unknown:
        raise ValueError(f"unknown raw timestamp labels: {unknown}")
    labels = labels[labels["label"] != "AMBIGUOUS"].copy()
    labels["target"] = labels["label"].isin(("LONG", "SHORT")).astype(np.int8)
    labels = labels.rename(columns={"label": "raw_label"})
    labels = labels.sort_values("decision_ts").reset_index(drop=True)
    if labels["decision_ts"].duplicated().any():
        raise ValueError("selected raw timestamp labels are not unique")
    labels["decision_ns"] = pd.DatetimeIndex(labels["decision_ts"]).as_unit("ns").asi8
    return labels[
        ["decision_ts", "decision_ns", "horizon_end_ts", "raw_label", "target"]
    ]


def build_aligned_market_frame(
    *,
    candles: pd.DataFrame,
    open_interest: pd.DataFrame,
    futures_metrics: pd.DataFrame,
    premium_index: pd.DataFrame,
    funding_features: pd.DataFrame,
) -> pd.DataFrame:
    candle_columns = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vol_ccy_quote",
        "confirm",
    ]
    source = candles[candle_columns].copy()
    source["timestamp"] = pd.to_datetime(source["timestamp"], utc=True)
    source = source[source["confirm"].fillna(0).astype(int) == 1]
    source = source.sort_values("timestamp").drop_duplicates("timestamp", keep="last")
    source = source.set_index("timestamp").drop(columns=["confirm"])
    full_index = pd.date_range(source.index.min(), source.index.max(), freq="5min")
    source = source.reindex(full_index)
    source.index.name = "timestamp"
    source["candle_coverage"] = source["close"].notna().astype(float)

    auxiliary = {
        "oi": _prepare_auxiliary(
            open_interest,
            value_columns=["sum_open_interest", "sum_open_interest_value"],
        ),
        "metrics": _prepare_auxiliary(
            futures_metrics,
            value_columns=[
                "top_trader_account_long_short_ratio",
                "top_trader_position_long_short_ratio",
                "global_account_long_short_ratio",
                "taker_buy_sell_volume_ratio",
            ],
        ),
        "premium": _prepare_auxiliary(
            premium_index,
            value_columns=[
                "premium_open",
                "premium_high",
                "premium_low",
                "premium_close",
            ],
        ),
        "funding": _prepare_auxiliary(
            funding_features,
            value_columns=[
                "latest_funding_rate",
                "funding_rate_change",
                "annualized_funding_rate",
                "funding_interval_hours",
                "funding_carry_1d",
                "funding_carry_3d",
                "funding_carry_7d",
                "funding_event_count_1d",
                "funding_event_count_3d",
                "funding_event_count_7d",
                "funding_rate_mean_7d",
                "funding_rate_std_7d",
                "funding_rate_zscore_7d",
                "funding_signed_streak",
                "funding_event_age_minutes",
                "minutes_to_expected_funding",
                "funding_event_is_new",
            ],
        ),
    }
    decision_ns = (source.index + SOURCE_INTERVAL).astype("int64").to_numpy()
    for prefix, frame in auxiliary.items():
        renamed = frame.rename(
            columns={
                column: f"{prefix}_{column}"
                for column in frame.columns
            }
        )
        source = source.join(renamed, how="left")
        available_column = f"{prefix}_available_at"
        available = pd.to_datetime(source[available_column], utc=True)
        available_ns = available.astype("int64").to_numpy()
        unavailable = available.isna().to_numpy() | (available_ns > decision_ns)
        value_columns = [
            column
            for column in source.columns
            if column.startswith(f"{prefix}_") and column != available_column
        ]
        source.loc[unavailable, value_columns] = np.nan
        source[f"{prefix}_coverage"] = (
            ~unavailable & source[value_columns].notna().any(axis=1).to_numpy()
        ).astype(float)
    return source


def _prepare_auxiliary(
    frame: pd.DataFrame,
    *,
    value_columns: list[str],
) -> pd.DataFrame:
    result = frame.copy()
    result["timestamp"] = pd.to_datetime(result["timestamp"], utc=True)
    if "confirm" in result:
        result = result[result["confirm"].fillna(0).astype(int) == 1]
    if "complete" in result:
        result = result[result["complete"].fillna(False).astype(bool)]
    if "available_at" not in result:
        result["available_at"] = result["timestamp"] + SOURCE_INTERVAL
    result["available_at"] = pd.to_datetime(result["available_at"], utc=True)
    for column in value_columns:
        result[column] = pd.to_numeric(result[column], errors="coerce").astype(float)
    result = result.sort_values("timestamp").drop_duplicates("timestamp", keep="last")
    return result.set_index("timestamp")[["available_at", *value_columns]]


def aggregate_market_resolution(
    source: pd.DataFrame,
    *,
    rule: str | None,
    expected_rows: int,
) -> pd.DataFrame:
    frame = source.copy()
    if rule is None:
        frame["oi_first"] = frame["oi_sum_open_interest"]
        frame["oi_min"] = frame["oi_sum_open_interest"]
        frame["oi_max"] = frame["oi_sum_open_interest"]
        for ratio in _ratio_columns():
            frame[f"{ratio}_mean"] = frame[ratio]
        return frame

    grouped = frame.resample(rule, origin="epoch", label="left", closed="left")
    result = pd.DataFrame(index=grouped["close"].last().index)
    result["open"] = grouped["open"].first()
    result["high"] = grouped["high"].max()
    result["low"] = grouped["low"].min()
    result["close"] = grouped["close"].last()
    result["volume"] = grouped["volume"].sum(min_count=1)
    result["vol_ccy_quote"] = grouped["vol_ccy_quote"].sum(min_count=1)
    oi = grouped["oi_sum_open_interest"]
    result["oi_first"] = oi.first()
    result["oi_sum_open_interest"] = oi.last()
    result["oi_min"] = oi.min()
    result["oi_max"] = oi.max()
    result["oi_sum_open_interest_value"] = grouped[
        "oi_sum_open_interest_value"
    ].last()
    for ratio in _ratio_columns():
        result[ratio] = grouped[ratio].last()
        result[f"{ratio}_mean"] = grouped[ratio].mean()
    result["premium_premium_open"] = grouped["premium_premium_open"].first()
    result["premium_premium_high"] = grouped["premium_premium_high"].max()
    result["premium_premium_low"] = grouped["premium_premium_low"].min()
    result["premium_premium_close"] = grouped["premium_premium_close"].last()
    funding_columns = [column for column in frame if column.startswith("funding_")]
    for column in funding_columns:
        result[column] = grouped[column].last()
    for coverage in ("oi", "metrics", "premium", "funding"):
        result[f"{coverage}_coverage"] = grouped[f"{coverage}_coverage"].mean()
    result["candle_coverage"] = (
        grouped["close"].count() / max(expected_rows, 1)
    ).clip(upper=1.0)
    return result


def build_market_features(frame: pd.DataFrame) -> pd.DataFrame:
    close = frame["close"].astype(float)
    open_ = frame["open"].astype(float)
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    previous_close = close.shift(1)
    candle_range = (high - low).replace(0, np.nan)
    quote_volume = frame["vol_ccy_quote"].astype(float).where(
        frame["vol_ccy_quote"].astype(float) > 0,
        frame["volume"].astype(float),
    )
    oi = frame["oi_sum_open_interest"].astype(float)
    oi_notional = frame["oi_sum_open_interest_value"].astype(float)
    features = pd.DataFrame(index=frame.index)
    features["price_log_return"] = _safe_log_ratio(close, previous_close)
    features["open_gap_log"] = _safe_log_ratio(open_, previous_close)
    features["price_range_log"] = _safe_log_ratio(high, low)
    features["price_body_log"] = _safe_log_ratio(close, open_)
    features["upper_wick_fraction"] = (
        high - pd.concat((open_, close), axis=1).max(axis=1)
    ) / candle_range
    features["lower_wick_fraction"] = (
        pd.concat((open_, close), axis=1).min(axis=1) - low
    ) / candle_range
    features["close_location"] = (close - low) / candle_range
    log_volume = np.log1p(quote_volume.clip(lower=0))
    features["log_quote_volume_change"] = log_volume.diff()
    features["quote_volume_zscore_60"] = rolling_zscore(log_volume, 60)
    log_oi = np.log(oi.where(oi > 0))
    features["oi_log_change"] = log_oi.diff()
    features["oi_level_zscore_60"] = rolling_zscore(log_oi, 60)
    log_oi_notional = np.log(oi_notional.where(oi_notional > 0))
    features["oi_notional_log_change"] = log_oi_notional.diff()
    features["oi_notional_zscore_60"] = rolling_zscore(log_oi_notional, 60)
    features["oi_range_log"] = _safe_log_ratio(frame["oi_max"], frame["oi_min"])
    for name, source_column in {
        "top_account": "metrics_top_trader_account_long_short_ratio",
        "top_position": "metrics_top_trader_position_long_short_ratio",
        "global": "metrics_global_account_long_short_ratio",
        "taker": "metrics_taker_buy_sell_volume_ratio",
    }.items():
        last = frame[source_column].astype(float)
        mean = frame[f"{source_column}_mean"].astype(float)
        features[f"{name}_log_ratio"] = np.log(last.where(last > 0))
        features[f"{name}_mean_log_ratio"] = np.log(mean.where(mean > 0))
        features[f"{name}_log_ratio_change"] = features[f"{name}_log_ratio"].diff()
    global_share = _long_share(frame["metrics_global_account_long_short_ratio"])
    features["top_account_global_long_share_gap"] = (
        _long_share(frame["metrics_top_trader_account_long_short_ratio"]) - global_share
    )
    features["top_position_global_long_share_gap"] = (
        _long_share(frame["metrics_top_trader_position_long_short_ratio"]) - global_share
    )
    features["top_position_account_long_share_gap"] = _long_share(
        frame["metrics_top_trader_position_long_short_ratio"]
    ) - _long_share(frame["metrics_top_trader_account_long_short_ratio"])
    premium_close = frame["premium_premium_close"].astype(float)
    features["premium_close"] = premium_close
    features["premium_change"] = premium_close.diff()
    features["premium_range"] = (
        frame["premium_premium_high"].astype(float)
        - frame["premium_premium_low"].astype(float)
    )
    features["premium_body"] = (
        premium_close - frame["premium_premium_open"].astype(float)
    )
    features["premium_zscore_60"] = rolling_zscore(premium_close, 60)
    direct_funding = {
        "funding_rate": "funding_latest_funding_rate",
        "funding_rate_change": "funding_funding_rate_change",
        "annualized_funding_rate": "funding_annualized_funding_rate",
        "funding_carry_1d": "funding_funding_carry_1d",
        "funding_carry_3d": "funding_funding_carry_3d",
        "funding_carry_7d": "funding_funding_carry_7d",
        "funding_rate_mean_7d": "funding_funding_rate_mean_7d",
        "funding_rate_std_7d": "funding_funding_rate_std_7d",
        "funding_zscore_7d": "funding_funding_rate_zscore_7d",
        "funding_event_count_1d": "funding_funding_event_count_1d",
        "funding_event_count_3d": "funding_funding_event_count_3d",
        "funding_event_count_7d": "funding_funding_event_count_7d",
        "funding_event_is_new": "funding_funding_event_is_new",
    }
    for output, source_column in direct_funding.items():
        features[output] = frame[source_column].astype(float)
    features["funding_signed_streak_scaled"] = np.tanh(
        frame["funding_funding_signed_streak"].astype(float) / 10.0
    )
    features["funding_event_age_scaled"] = (
        frame["funding_funding_event_age_minutes"].astype(float) / 480.0
    ).clip(0, 4)
    features["minutes_to_expected_funding_scaled"] = (
        frame["funding_minutes_to_expected_funding"].astype(float) / 480.0
    ).clip(-2, 2)
    for coverage in ("candle", "oi", "metrics", "premium", "funding"):
        features[f"{coverage}_coverage"] = frame[f"{coverage}_coverage"].astype(float)
    return features.replace([np.inf, -np.inf], np.nan)


def build_funding_event_features(
    raw_funding: pd.DataFrame,
) -> tuple[pd.DataFrame, np.ndarray]:
    frame = raw_funding.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame[frame["confirm"].fillna(0).astype(int) == 1]
    frame = frame.sort_values("timestamp").drop_duplicates("timestamp", keep="last")
    frame = frame.set_index("timestamp")
    rate = frame["funding_rate"].astype(float)
    features = pd.DataFrame(index=frame.index)
    features["funding_rate"] = rate
    features["funding_rate_change"] = rate.diff()
    features["funding_rate_mean_20"] = rate.rolling(20, min_periods=8).mean()
    features["funding_rate_std_20"] = rate.rolling(20, min_periods=8).std(ddof=1)
    features["funding_rate_zscore_20"] = rolling_zscore(rate, 20, minimum=8)
    features["funding_sign"] = np.sign(rate)
    features["funding_interval_scaled"] = (
        frame["funding_interval_hours"].astype(float) / 8.0
    )
    available_ns = (frame.index + pd.Timedelta(minutes=5)).astype("int64").to_numpy()
    return features, available_ns


def fit_preprocessor(
    timelines: Mapping[str, FeatureTimeline],
    *,
    train_end: pd.Timestamp,
    clip_value: float,
) -> PreprocessorState:
    train_end_ns = int(train_end.value)
    branches: dict[str, BranchPreprocessorState] = {}
    for name, timeline in timelines.items():
        rows = timeline.values[timeline.available_ns <= train_end_ns].astype(np.float64)
        if not len(rows):
            raise ValueError(f"{name} has no rows available by {train_end}")
        center = np.zeros(rows.shape[1], dtype=np.float32)
        scale = np.ones(rows.shape[1], dtype=np.float32)
        for column in range(rows.shape[1]):
            finite = rows[np.isfinite(rows[:, column]), column]
            if not len(finite):
                continue
            median = float(np.median(finite))
            q25, q75 = np.quantile(finite, (0.25, 0.75))
            robust_scale = float(q75 - q25)
            if not np.isfinite(robust_scale) or robust_scale <= 1e-12:
                robust_scale = float(np.std(finite))
            if not np.isfinite(robust_scale) or robust_scale <= 1e-12:
                robust_scale = 1.0
            center[column] = median
            scale[column] = robust_scale
        branches[name] = BranchPreprocessorState(
            channel_names=timeline.channel_names,
            center=center,
            scale=scale,
            clip_value=float(clip_value),
            fitted_through_ns=train_end_ns,
        )
    return PreprocessorState(
        schema_version=FEATURE_SCHEMA_VERSION,
        branches=branches,
    )


def transform_timelines(
    timelines: Mapping[str, FeatureTimeline],
    preprocessor: PreprocessorState,
) -> dict[str, TransformedTimeline]:
    if preprocessor.schema_version != FEATURE_SCHEMA_VERSION:
        raise ValueError("preprocessor feature schema does not match")
    transformed: dict[str, TransformedTimeline] = {}
    for name, timeline in timelines.items():
        state = preprocessor.branches[name]
        if timeline.channel_names != state.channel_names:
            raise ValueError(f"{name} channel order changed")
        present = np.isfinite(timeline.values)
        values = (timeline.values - state.center) / state.scale
        values = np.clip(values, -state.clip_value, state.clip_value)
        values = np.where(present, values, 0.0).astype(np.float32)
        mask = present.astype(np.float32)
        combined = np.concatenate((values, mask), axis=1)
        channel_names = tuple(
            [f"value:{channel}" for channel in timeline.channel_names]
            + [f"present:{channel}" for channel in timeline.channel_names]
        )
        transformed[name] = TransformedTimeline(
            name=name,
            available_ns=timeline.available_ns,
            values=np.ascontiguousarray(combined),
            channel_names=channel_names,
            spec=timeline.spec,
        )
    return transformed


def build_sample_index(
    labels: pd.DataFrame,
    timelines: Mapping[str, FeatureTimeline | TransformedTimeline],
    *,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
) -> pd.DataFrame:
    selected = labels.copy()
    if start is not None:
        selected = selected[selected["decision_ts"] >= start]
    if end is not None:
        selected = selected[selected["decision_ts"] <= end]
    decision_ns = selected["decision_ns"].to_numpy(dtype=np.int64)
    ready = np.ones(len(selected), dtype=bool)
    for timeline in timelines.values():
        latest = np.searchsorted(timeline.available_ns, decision_ns, side="right") - 1
        ready &= latest >= timeline.spec.steps - 1
    return selected.loc[ready].reset_index(drop=True)


def build_data_audit(
    *,
    labels: pd.DataFrame,
    timelines: Mapping[str, FeatureTimeline],
    preprocessor: PreprocessorState | None = None,
) -> dict[str, Any]:
    branches: dict[str, Any] = {}
    for name, timeline in timelines.items():
        missing = np.mean(~np.isfinite(timeline.values), axis=0)
        branch: dict[str, Any] = {
            "rows": int(len(timeline.values)),
            "raw_channels": int(len(timeline.channel_names)),
            "tensor_channels_with_masks": int(2 * len(timeline.channel_names)),
            "sequence_steps": int(timeline.spec.steps),
            "lookback_days": round(timeline.spec.lookback_days, 6),
            "first_available_ns": int(timeline.available_ns[0]),
            "last_available_ns": int(timeline.available_ns[-1]),
            "missing_fraction_by_channel": {
                channel: round(float(fraction), 8)
                for channel, fraction in zip(timeline.channel_names, missing)
            },
        }
        if preprocessor is not None:
            state = preprocessor.branches[name]
            present = np.isfinite(timeline.values)
            scaled = (timeline.values - state.center) / state.scale
            clipped = present & (np.abs(scaled) > state.clip_value)
            denominator = max(int(present.sum()), 1)
            branch["clipped_fraction"] = round(float(clipped.sum() / denominator), 8)
            branch["unit_scale_channels"] = [
                channel
                for channel, scale in zip(state.channel_names, state.scale)
                if float(scale) == 1.0
            ]
        branches[name] = branch
    return {
        "schema_version": FEATURE_SCHEMA_VERSION,
        "labels": {
            "eligible_rows": int(len(labels)),
            "positive_rows": int(labels["target"].sum()),
            "negative_rows": int((labels["target"] == 0).sum()),
            "positive_prevalence": round(float(labels["target"].mean()), 8),
            "raw_label_counts": {
                str(label): int(count)
                for label, count in labels["raw_label"].value_counts().items()
            },
            "episode_fields_present": any(
                "episode" in str(column).lower() for column in labels.columns
            ),
        },
        "branches": branches,
    }


def prepare_supervised_training_data(
    *,
    workspace_root: Path,
    artifact_root: Path,
) -> dict[str, Any]:
    module = TimestampTrainingDataModule.from_session(
        workspace_root=workspace_root,
        artifact_root=artifact_root,
    )
    sources = module.load_sources()
    labels = module.load_labels()
    timelines = module.build_feature_timelines(sources)
    sample_index = module.build_sample_index(labels, timelines)
    audit = module.audit(labels=sample_index, timelines=timelines)

    output_root = module.artifact_root / "training" / "supervised_input"
    output_root.mkdir(parents=True, exist_ok=True)
    labels_path = output_root / "timestamp_labels.parquet"
    _atomic_write_parquet(labels_path, sample_index)

    branch_artifacts: dict[str, Any] = {}
    for name, timeline in timelines.items():
        branch_root = output_root / name
        branch_root.mkdir(parents=True, exist_ok=True)
        available_path = branch_root / "available_ns.npy"
        values_path = branch_root / "values.npy"
        _atomic_write_npy(available_path, timeline.available_ns)
        _atomic_write_npy(values_path, timeline.values)
        branch_artifacts[name] = {
            "available_ns_path": str(available_path.relative_to(module.artifact_root)),
            "available_ns_sha256": _sha256(available_path),
            "values_path": str(values_path.relative_to(module.artifact_root)),
            "values_sha256": _sha256(values_path),
            "channel_names": list(timeline.channel_names),
            "spec": {
                "name": timeline.spec.name,
                "rule": timeline.spec.rule,
                "interval_minutes": timeline.spec.interval_minutes,
                "steps": timeline.spec.steps,
                "source": timeline.spec.source,
                "lookback_days": round(timeline.spec.lookback_days, 6),
            },
        }

    manifest: dict[str, Any] = {
        "schema_version": "motis_supervised_training_input.v1",
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "session_id": module.session_id,
        "target_config_hash": module.target_config_hash,
        "module_import": MODULE_IMPORT,
        "artifact_root": str(module.artifact_root),
        "dataset_ids": module.dataset_ids,
        "target": {
            "risk_pct": module.config.target.risk_pct,
            "reward_multiple": module.config.target.reward_multiple,
            "stop_multiple": module.config.target.stop_multiple,
            "entry_delay_minutes": module.config.target.entry_delay_minutes,
            "horizon_hours": module.config.target.horizon_hours,
            "positive_raw_labels": ["LONG", "SHORT"],
            "negative_raw_labels": ["NEUTRAL"],
            "excluded_raw_labels": ["AMBIGUOUS"],
            "base_sample_weight": 1.0,
            "episodes_used": False,
        },
        "splits": {
            "research_start": module.config.research_start.isoformat(),
            "research_end": module.config.research_end.isoformat(),
            "walk_forward_start": module.config.walk_forward_start.isoformat(),
        },
        "labels": {
            "path": str(labels_path.relative_to(module.artifact_root)),
            "sha256": _sha256(labels_path),
            **audit["labels"],
        },
        "branches": branch_artifacts,
        "preprocessing": {
            "normalization": "per_branch_channel_median_iqr",
            "fit_scope": "unique feature rows in each chronological training fold only",
            "clip_value": module.config.clip_value,
            "missing_value_after_scaling": 0.0,
            "presence_mask_per_channel": True,
            "precomputed_global_scaler": False,
        },
        "audit": audit,
    }
    canonical = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    manifest["manifest_hash"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    manifest_path = output_root / "manifest.json"
    _atomic_write_text(manifest_path, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return {
        "manifest": manifest,
        "manifest_path": str(manifest_path),
        "labels_path": str(labels_path),
    }


def load_prepared_supervised_training_data(
    manifest_path: Path,
) -> tuple[pd.DataFrame, dict[str, FeatureTimeline], dict[str, Any]]:
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("schema_version") != "motis_supervised_training_input.v1":
        raise ValueError("unsupported supervised training input manifest")
    artifact_root = Path(manifest["artifact_root"])
    labels_path = artifact_root / manifest["labels"]["path"]
    if _sha256(labels_path) != manifest["labels"]["sha256"]:
        raise ValueError("prepared timestamp label hash changed")
    labels = pq.read_table(labels_path).to_pandas()
    labels["decision_ts"] = pd.to_datetime(labels["decision_ts"], utc=True)
    labels["horizon_end_ts"] = pd.to_datetime(labels["horizon_end_ts"], utc=True)
    timelines: dict[str, FeatureTimeline] = {}
    for name, entry in manifest["branches"].items():
        available_path = artifact_root / entry["available_ns_path"]
        values_path = artifact_root / entry["values_path"]
        if _sha256(available_path) != entry["available_ns_sha256"]:
            raise ValueError(f"{name} prepared availability hash changed")
        if _sha256(values_path) != entry["values_sha256"]:
            raise ValueError(f"{name} prepared value hash changed")
        spec_value = entry["spec"]
        spec = BranchSpec(
            name=str(spec_value["name"]),
            rule=spec_value.get("rule"),
            interval_minutes=int(spec_value["interval_minutes"]),
            steps=int(spec_value["steps"]),
            source=str(spec_value["source"]),
        )
        timelines[name] = FeatureTimeline(
            name=name,
            available_ns=np.load(available_path, mmap_mode="r"),
            values=np.load(values_path, mmap_mode="r"),
            channel_names=tuple(entry["channel_names"]),
            spec=spec,
        )
    return labels, timelines, manifest


def _atomic_write_parquet(path: Path, frame: pd.DataFrame) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def _atomic_write_npy(path: Path, values: np.ndarray) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("wb") as handle:
        np.save(handle, np.asarray(values), allow_pickle=False)
    temporary.replace(path)


def _atomic_write_text(path: Path, value: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value)
    temporary.replace(path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def rolling_zscore(
    values: pd.Series,
    window: int,
    *,
    minimum: int | None = None,
) -> pd.Series:
    minimum = minimum or max(8, window // 3)
    prior = values.shift(1)
    mean = prior.rolling(window, min_periods=minimum).mean()
    std = prior.rolling(window, min_periods=minimum).std(ddof=1)
    return (values - mean) / std.replace(0, np.nan)


def _safe_log_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    numerator = numerator.astype(float)
    denominator = denominator.astype(float)
    valid = (numerator > 0) & (denominator > 0)
    result = pd.Series(np.nan, index=numerator.index, dtype=float)
    result.loc[valid] = np.log(numerator.loc[valid] / denominator.loc[valid])
    return result


def _long_share(ratio: pd.Series) -> pd.Series:
    ratio = ratio.astype(float)
    return ratio / (1.0 + ratio)


def _ratio_columns() -> tuple[str, ...]:
    return (
        "metrics_top_trader_account_long_short_ratio",
        "metrics_top_trader_position_long_short_ratio",
        "metrics_global_account_long_short_ratio",
        "metrics_taker_buy_sell_volume_ratio",
    )
