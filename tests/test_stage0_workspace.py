from __future__ import annotations

import json

from quant_terminal_worker.stage0.workspace import (
    build_stage0_commands,
    materialize_stage0_workspace,
)


def test_materialize_stage0_workspace_writes_canonical_skill_inputs(tmp_path):
    signal_set = {
        "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
        "signal_set_id": "2026-BTC-2h-dedupe-vote2",
        "signal_engine_id": "vegas_ema",
        "asset": "BTC",
        "manifest": {
            "signal_set_id": "2026-BTC-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "asset": "BTC",
            "parameters": {"vote_threshold": 2},
        },
    }
    signals = [
        {
            "signal_id": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2:20260301T000000Z",
            "timestamp": "2026-03-01T00:00:00Z",
            "payload": {
                "schema_version": "signal_packet.v2",
                "asset": "BTC",
                "timestamp": "2026-03-01T00:00:00Z",
                "interactions": [{"market_price": "100"}],
            },
        }
    ]
    candles = [
        {
            "timestamp": "2026-03-01T00:05:00Z",
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1.0,
            "vol_ccy": 1.0,
            "vol_ccy_quote": 100.5,
            "confirm": 1,
        }
    ]

    result = materialize_stage0_workspace(
        workspace_root=tmp_path,
        strategy_id="btc-vegas-tunnel-v01",
        signal_set=signal_set,
        signals=signals,
        candle_rows=candles,
    )

    stage0_dir = tmp_path / "dev/training_sessions/btc-vegas-tunnel-v01/stage0/2026-BTC-2h-dedupe-vote2"
    packet_path = stage0_dir / "scores/_scoreable_signal_subset/packets/20260301T000000Z.json"
    candle_path = tmp_path / "dev/data/raw/BTC/5m/candles.csv"
    manifest_path = tmp_path / "dev/signals/vegas_ema/BTC/2026-BTC-2h-dedupe-vote2/manifest.json"

    assert result["signal_packets_dir"] == str(packet_path.parent)
    assert result["candles_csv"] == str(candle_path)
    assert result["stage0_dir"] == str(stage0_dir)
    assert json.loads(packet_path.read_text()) == signals[0]["payload"]
    assert json.loads(manifest_path.read_text()) == signal_set["manifest"]
    assert candle_path.read_text().splitlines()[0] == "ts,open,high,low,close,volume,vol_ccy,vol_ccy_quote,confirm"
    assert "2026-03-01T00:05:00Z,100.0,101.0,99.0,100.5,1.0,1.0,100.5,1" in candle_path.read_text()


def test_build_stage0_commands_uses_canonical_skill_scripts(tmp_path):
    commands = build_stage0_commands(
        workspace_root=tmp_path,
        strategy_id="btc-vegas-tunnel-v01",
        asset="BTC",
        signal_engine_id="vegas_ema",
        signal_set_id="2026-BTC-2h-dedupe-vote2",
        signal_packets_dir=str(tmp_path / "dev/signals/vegas_ema/BTC/2026-BTC-2h-dedupe-vote2/packets"),
        candles_csv=str(tmp_path / "dev/data/raw/BTC/5m/candles.csv"),
        forward_hours=36,
        vote_threshold=2,
        significance_threshold_pct=0.9,
    )

    assert commands["stage0a"][1].endswith("scripts/optimization/max_travel_distribution.py")
    assert commands["stage0b"][1].endswith("scripts/optimization/significance_threshold_calibration.py")
    assert commands["stage0c"][1].endswith("scripts/optimization/signal_ground_truth.py")
    assert "--forward-hours" in commands["stage0a"]
    assert "36" in commands["stage0a"]
    assert "--significance-threshold" in commands["stage0c"]
    assert "0.9" in commands["stage0c"]
