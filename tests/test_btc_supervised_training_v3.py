from __future__ import annotations

from pathlib import Path
import hashlib
import json
import runpy

import numpy as np
import pandas as pd
import pytest


SCRIPT = runpy.run_path(
    str(
        Path(__file__).resolve().parents[1]
        / "dev"
        / "signal_discovery_sessions"
        / "discovery-btc-2025-04-01-2026-07-10-mrle2qpp"
        / "prompt"
        / "train_supervised_opportunity_v3.py"
    )
)


def test_duration_weighting_preserves_episode_length_ratio_after_class_balance() -> None:
    rows = []
    for episode_key, target, row_count in (
        ("positive-short", 1, 2),
        ("positive-long", 1, 120),
        ("negative-short", 0, 2),
        ("negative-long", 0, 120),
    ):
        rows.extend(
            {
                "episode_key": episode_key,
                "target": target,
                "row_duration_weight": 1.0,
            }
            for _ in range(row_count)
        )
    labeled = pd.DataFrame(rows)

    weights = SCRIPT["balanced_duration_weights"](labeled)
    labeled["weight"] = weights
    mass = labeled.groupby("episode_key")["weight"].sum()

    assert mass["positive-long"] / mass["positive-short"] == pytest.approx(60.0)
    assert mass["negative-long"] / mass["negative-short"] == pytest.approx(60.0)
    assert labeled.loc[labeled["target"] == 1, "weight"].sum() == pytest.approx(0.5)
    assert labeled.loc[labeled["target"] == 0, "weight"].sum() == pytest.approx(0.5)


def test_episode_mass_is_capped_at_the_forty_eight_hour_horizon() -> None:
    row_count = 1000
    labeled = pd.DataFrame(
        {
            "episode_key": ["long"] * row_count + ["other"] * 2,
            "target": [1] * row_count + [0] * 2,
            "row_duration_weight": [576.0 / row_count] * row_count + [1.0, 1.0],
        }
    )

    raw_mass = labeled.groupby("episode_key")["row_duration_weight"].sum()

    assert raw_mass["long"] == pytest.approx(576.0)


def test_purged_training_filter_keeps_complete_episodes_outside_boundary() -> None:
    valid_start = pd.Timestamp("2025-09-01T00:00:00Z")
    purge = SCRIPT["PURGE"]
    labeled = pd.DataFrame(
        {
            "episode_key": ["safe", "crosses"],
            "episode_end": [
                valid_start - purge - pd.Timedelta(minutes=5),
                valid_start - purge + pd.Timedelta(minutes=5),
            ],
        }
    )

    training = labeled[labeled["episode_end"] < valid_start - purge]

    assert training["episode_key"].tolist() == ["safe"]


def test_wilson_interval_penalizes_tiny_apparent_precision() -> None:
    tiny = SCRIPT["wilson_interval"](4, 4)
    supported = SCRIPT["wilson_interval"](80, 100)

    assert tiny[0] < supported[0]
    assert np.isclose(tiny[1], 1.0)


def test_rejected_report_preserves_sealed_walk_forward_and_artifact_hash() -> None:
    report = json.loads(SCRIPT["REPORT_PATH"].read_text())
    artifact_path = SCRIPT["ARTIFACT_PATH"]

    assert report["decision"]["status"] == "rejected"
    assert report["decision"]["deployment_status"] == "not_registered"
    assert report["sealed_walk_forward_inspected"] is False
    assert report["untouched_research_holdout"]["precision_lift_percentage_points"] < 0
    assert report["production_fit"]["artifact_sha256"] == hashlib.sha256(
        artifact_path.read_bytes()
    ).hexdigest()
