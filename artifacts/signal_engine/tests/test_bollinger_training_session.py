from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_generate_bollinger_training_session_writes_individual_packets(tmp_path) -> None:
    out_dir = tmp_path / "bollinger_signals"

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "signals" / "generate_bollinger_training_session.py"),
            "--asset",
            "BTC",
            "--start",
            "2025-04-01T00:00:00Z",
            "--end",
            "2025-04-03T00:00:00Z",
            "--out-dir",
            str(out_dir),
            "--context-bars",
            "20",
            "--ema-warmup-bars",
            "30",
            "--bb-period",
            "20",
            "--proximity-threshold",
            "1",
            "--vote-threshold",
            "1",
            "--window-minutes",
            "120",
            "--timeframes",
            "4h",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    summary = json.loads(result.stdout)
    assert summary["asset"] == "BTC"
    assert summary["strategy"] == "bollinger"
    assert summary["raw_signals"] > 0
    assert summary["dedup_emitted"] > 0

    signal_files = sorted(out_dir.glob("*.json"))
    assert len(signal_files) == summary["dedup_emitted"]
    packet = json.loads(signal_files[0].read_text())
    assert set(packet) == {
        "schema_version",
        "asset",
        "timestamp",
        "active_timeframes",
        "interactions",
        "charts",
    }
    assert packet["schema_version"] == "signal_packet.v2"
    interaction = next(
        interaction
        for interaction in packet["interactions"]
        if interaction["timeframe"] == "4h"
    )
    assert "band" in interaction
    assert "band_upper_limit" in interaction
    assert "tunnel" not in interaction
