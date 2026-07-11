from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path


def _load_ground_truth_module():
    path = Path("artifacts/skills/agentic-quant-trading-development/scripts/optimization/signal_ground_truth.py")
    spec = importlib.util.spec_from_file_location("signal_ground_truth_script", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_terminal_fallback_label_mode_uses_cutoff_direction_for_no_trigger():
    module = _load_ground_truth_module()
    candles = [
        {"ts": _ts("2026-03-01T00:05:00Z"), "open": 100.0, "high": 100.4, "low": 99.6, "close": 100.1},
        {"ts": _ts("2026-03-01T00:30:00Z"), "open": 100.1, "high": 101.0, "low": 100.0, "close": 101.0},
        {"ts": _ts("2026-03-01T01:00:00Z"), "open": 101.0, "high": 101.2, "low": 100.8, "close": 101.0},
    ]

    result = module.analyze_signal(
        candles,
        _ts("2026-03-01T00:00:00Z"),
        100.0,
        1,
        2.0,
        label_mode="terminal_fallback",
    )

    assert result["status"] == "terminal_fallback"
    assert result["natural_direction"] == "LONG"
    assert result["label_source"] == "terminal_fallback"
    assert result["threshold_passed"] is False
    assert result["terminal_direction"] == "LONG"
    assert result["terminal_return_pct"] == 1.0

