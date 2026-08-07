from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from quant_terminal_worker.execution import atr_source
from quant_terminal_worker.execution.atr_source import resolve_atr_policy


class FakeMarketDataRepository:
    def get_ref(self, dataset_id: str):
        if dataset_id == "btc-atr-2h":
            return {
                "dataset_id": "btc-atr-2h",
                "data_type": "technical_indicator_atr",
                "storage_backend": "parquet",
                "storage_uri": ".data/atr.parquet",
            }
        return None

    def get_data_ref(self, *, asset: str, timeframe: str, origin: str, data_type: str):
        if (asset, timeframe, origin, data_type) == ("BTC", "2h", "derived", "technical_indicator_atr"):
            return self.get_ref("btc-atr-2h")
        return None


def test_resolve_atr_policy_uses_latest_available_at_before_signal(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    atr_source._cached_atr_series.cache_clear()
    monkeypatch.setattr(
        atr_source,
        "read_rows_from_ref",
        lambda *args, **kwargs: [
            {
                "timestamp": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
                "available_at": datetime(2026, 5, 1, 2, 0, tzinfo=UTC),
                "atr_pct": 1.0,
                "warmup_complete": True,
            },
            {
                "timestamp": datetime(2026, 5, 1, 2, 0, tzinfo=UTC),
                "available_at": datetime(2026, 5, 1, 4, 0, tzinfo=UTC),
                "atr_pct": 2.0,
                "warmup_complete": True,
            },
        ],
    )

    resolved = resolve_atr_policy(
        {
            "atr_source": {
                "timeframe": "2h",
                "tp_multiplier": 1.5,
                "sl_multiplier": 0.75,
            }
        },
        signal_timestamp=datetime(2026, 5, 1, 3, 0, tzinfo=UTC),
        market_data_repository=FakeMarketDataRepository(),
        workspace_root=tmp_path,
        asset="BTC",
    )

    assert resolved.policy["final_tp_pct"] == 1.5
    assert resolved.policy["initial_sl_pct"] == 0.75
    assert resolved.diagnostics["atr_source"]["dataset_id"] == "btc-atr-2h"
    assert resolved.diagnostics["atr_source"]["atr_available_at"] == "2026-05-01T02:00:00Z"
