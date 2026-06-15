from __future__ import annotations

from typing import Any


JOB_QUEUE_BY_TYPE = {
    "market_data_refresh": "market_data",
    "market_data_ema_refresh": "market_data",
    "market_data_feature_refresh": "market_data",
    "signal_pool_extend": "signal_generation",
    "stage0_candidate": "research",
    "stage0_candidate_batch": "research",
    "stage1_canonical": "research",
    "stage1_score": "research",
    "stage2_capture_curve": "research",
    "stage3_policy_step": "research",
    "stage3_pyramid": "research",
    "stage4_realized_expectancy": "research",
}


def queue_for_job(job_type: str, payload: dict[str, Any] | None = None) -> str:
    del payload
    return JOB_QUEUE_BY_TYPE.get(job_type, "default")
