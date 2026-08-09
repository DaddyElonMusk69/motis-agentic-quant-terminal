from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from quant_terminal_sdk.market_data_reader import MarketDataReader


DEFAULT_CANONICAL_SIGNAL_FRESHNESS_MINUTES = 10
DEFAULT_PENDING_CANONICAL_SIGNAL_LIMIT = 1000


def load_pending_canonical_entry_signals(
    *,
    route: dict[str, Any],
    bundle: dict[str, Any],
    repository: Any,
    workspace_root: Path,
    after_timestamp: datetime,
    limit: int = DEFAULT_PENDING_CANONICAL_SIGNAL_LIMIT,
) -> dict[str, Any]:
    context = resolve_route_canonical_signal_context(route=route, bundle=bundle, repository=repository)
    signal_set_key = context["signal_set_key"]
    latest_confirmed_candle_ts = _latest_confirmed_candle_ts(
        repository=repository,
        workspace_root=workspace_root,
        asset=route["asset"],
    )
    signals = repository.list_signals(
        signal_set_key=signal_set_key,
        after_timestamp=after_timestamp,
        through_timestamp=latest_confirmed_candle_ts,
        limit=limit,
        descending=False,
    )
    pending = [
        dict(signal)
        for signal in signals
        if after_timestamp < _parse_timestamp(signal["timestamp"]) <= latest_confirmed_candle_ts
    ]
    pending.sort(key=lambda signal: (_parse_timestamp(signal["timestamp"]), str(signal.get("signal_id") or "")))
    return {
        "signals": pending,
        "scan_result": {
            "status": "pending_canonical_signals" if pending else "no_fresh_canonical_signal",
            "source": "canonical_signal_pool",
            "signal_set_key": signal_set_key,
            "cursor_timestamp": _iso_z(after_timestamp),
            "latest_confirmed_candle_ts": _iso_z(latest_confirmed_candle_ts),
            "pending_count": len(pending),
            "truncated": len(pending) >= limit,
        },
    }


def load_latest_canonical_entry_signal(
    *,
    route: dict[str, Any],
    bundle: dict[str, Any],
    repository: Any,
    workspace_root: Path,
    freshness_minutes: int = DEFAULT_CANONICAL_SIGNAL_FRESHNESS_MINUTES,
) -> dict[str, Any]:
    context = resolve_route_canonical_signal_context(route=route, bundle=bundle, repository=repository)
    signal_set_key = context["signal_set_key"]
    latest_confirmed_candle_ts = _latest_confirmed_candle_ts(
        repository=repository,
        workspace_root=workspace_root,
        asset=route["asset"],
    )
    signals = repository.list_signals(signal_set_key=signal_set_key, limit=1, descending=True)
    if not signals:
        return {
            "signal": None,
            "scan_result": {
                "status": "no_fresh_canonical_signal",
                "source": "canonical_signal_pool",
                "signal_set_key": signal_set_key,
                "latest_confirmed_candle_ts": _iso_z(latest_confirmed_candle_ts),
            },
        }

    signal = dict(signals[0])
    signal_ts = _parse_timestamp(signal["timestamp"])
    freshness_seconds = int((latest_confirmed_candle_ts - signal_ts).total_seconds())
    scan_result = {
        "status": "fresh_signal",
        "source": "canonical_signal_pool",
        "signal_id": signal["signal_id"],
        "signal_set_key": signal_set_key,
        "signal_timestamp": _iso_z(signal_ts),
        "latest_confirmed_candle_ts": _iso_z(latest_confirmed_candle_ts),
        "freshness_seconds": freshness_seconds,
        "freshness_class": "fresh" if freshness_seconds <= 300 else "late",
        "signal_engine_id": signal.get("signal_engine_id") or route.get("signal_engine_id"),
        "asset": signal.get("asset") or route.get("asset"),
    }
    if signal_ts > latest_confirmed_candle_ts:
        return {"signal": None, "scan_result": {**scan_result, "status": "future_canonical_signal"}}
    if freshness_seconds > int(timedelta(minutes=freshness_minutes).total_seconds()):
        return {"signal": None, "scan_result": {**scan_result, "status": "late_stale_canonical_signal"}}
    return {"signal": signal, "scan_result": scan_result}


def resolve_route_canonical_signal_context(
    *,
    route: dict[str, Any],
    bundle: dict[str, Any],
    repository: Any,
) -> dict[str, Any]:
    source_session_id = bundle.get("source_stage1_session_id") or route.get("source_stage1_session_id")
    if not source_session_id:
        raise ValueError("active bundle is missing source_stage1_session_id for canonical signal admission")
    if not hasattr(repository, "get_stage1_research_session"):
        raise ValueError("repository cannot resolve Stage 1 session for canonical signal admission")
    session = repository.get_stage1_research_session(source_session_id)
    if session is None:
        raise ValueError(f"source Stage 1 session not found for canonical signal admission: {source_session_id}")
    signal_set_key = session.get("signal_set_key")
    if not signal_set_key:
        raise ValueError(f"source Stage 1 session is missing signal_set_key: {source_session_id}")
    if hasattr(repository, "get_signal_set") and repository.get_signal_set(signal_set_key) is None:
        raise ValueError(f"canonical signal set not found for live route: {signal_set_key}")
    return {
        "source_stage1_session_id": source_session_id,
        "signal_set_key": signal_set_key,
        "stage1_session": session,
    }


def _latest_confirmed_candle_ts(
    *,
    repository: Any,
    workspace_root: Path,
    asset: str,
) -> datetime:
    getter = getattr(repository, "get_latest_confirmed_candle_timestamp", None)
    if callable(getter):
        return _parse_timestamp(getter(asset=asset, timeframe="5m", origin="raw"))
    candles = MarketDataReader(repository=repository, workspace_root=workspace_root).get_candles(
        asset=asset,
        timeframe="5m",
        origin="raw",
        confirmed_only=True,
    )
    if not candles:
        raise ValueError(f"no confirmed raw 5m candles available for canonical signal admission: {asset}")
    return candles[-1].timestamp


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _iso_z(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
