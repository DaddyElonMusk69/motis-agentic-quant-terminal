from fastapi.testclient import TestClient

from quant_terminal_api.main import create_app


class FakeRuntimeRepository:
    def __init__(self) -> None:
        self.engines = []
        self.strategies = []
        self.persisted = []

    def register_signal_engine(self, registration):
        self.engines.append(registration)

    def register_strategy(self, registration):
        self.strategies.append(registration)

    def persist_stage1_backtest(self, result):
        self.persisted.append(result)

    def get_backtest_run(self, run_id):
        if run_id == "bt-api-1":
            return {
                "run_id": "bt-api-1",
                "status": "completed",
                "metrics": {"total": 1, "agreement_rate": 1.0},
                "decision_count": 1,
                "signal_count": 1,
            }
        return None


def test_backtest_api_registers_modules_and_launches_stage1_run():
    repository = FakeRuntimeRepository()
    client = TestClient(create_app(runtime_repository=repository))

    engine_response = client.post(
        "/api/v1/signal-engines/register",
        json={
            "signal_engine_id": "threshold_reversal",
            "name": "Threshold Reversal",
            "version": "0.1.0",
            "runtime_entrypoint": "quant_terminal_engines.threshold_reversal:generate_signals",
        },
    )
    strategy_response = client.post(
        "/api/v1/strategies/register",
        json={
            "strategy_id": "directional_threshold",
            "name": "Directional Threshold",
            "version": "0.1.0",
            "runtime_entrypoint": "quant_terminal_strategies.directional_threshold:decide",
        },
    )
    run_response = client.post(
        "/api/v1/backtests/stage1",
        json={
            "run_id": "bt-api-1",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "dataset_refs": ["btc-raw-5m"],
            "rows": [
                {"timestamp": "2026-06-01T00:00:00Z", "open": 100, "close": 100},
                {"timestamp": "2026-06-01T00:05:00Z", "open": 100, "close": 103},
            ],
            "signal_engine": {
                "signal_engine_id": "threshold_reversal",
                "version": "0.1.0",
                "runtime_entrypoint": "quant_terminal_engines.threshold_reversal:generate_signals",
                "parameters": {"min_move_pct": 1.0},
            },
            "strategy": {
                "strategy_id": "directional_threshold",
                "version": "0.1.0",
                "runtime_entrypoint": "quant_terminal_strategies.directional_threshold:decide",
                "parameters": {"long_threshold_pct": 1.0, "short_threshold_pct": -1.0},
            },
            "ground_truth": {"threshold_reversal-BTC-20260601T000500Z": "LONG"},
        },
    )
    summary_response = client.get("/api/v1/backtests/bt-api-1")

    assert engine_response.status_code == 200
    assert strategy_response.status_code == 200
    assert run_response.status_code == 200
    assert run_response.json()["score_summary"]["metrics"]["agreement_rate"] == 1.0
    assert repository.persisted[0]["run_id"] == "bt-api-1"
    assert summary_response.status_code == 200
    assert summary_response.json()["decision_count"] == 1
