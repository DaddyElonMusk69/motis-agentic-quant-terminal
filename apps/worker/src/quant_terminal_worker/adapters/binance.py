from __future__ import annotations

from dataclasses import dataclass
import json
from json import JSONDecodeError
import os
import shlex
import shutil
import subprocess
from typing import Any, Callable
from urllib.parse import urlencode

from quant_terminal_worker.adapters.exchange import ExchangeAdapterError


class BinanceCLIError(ExchangeAdapterError):
    pass


CommandRunner = Callable[[list[str], Any], str]
FUTURES_DATA_BASE_URL = "https://fapi.binance.com/futures/data"
FUTURES_API_BASE_URL = "https://fapi.binance.com/fapi/v1"
FUTURES_HISTORY_REST_PATHS = {
    "open-interest-statistics": "openInterestHist",
    "top-trader-long-short-ratio-accounts": "topLongShortAccountRatio",
    "top-trader-long-short-ratio-positions": "topLongShortPositionRatio",
    "long-short-ratio": "globalLongShortAccountRatio",
    "taker-buy-sell-volume": "takerlongshortRatio",
}


def _default_command_runner(command: list[str], *, timeout_seconds: int) -> str:
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if completed.returncode != 0:
        details = "\n".join(part.strip() for part in (completed.stderr, completed.stdout) if part.strip())
        raise BinanceCLIError(details or f"Binance CLI command failed: {shlex.join(command)}")
    return completed.stdout


def _request_binance_json(
    *,
    base_url: str,
    rest_path: str,
    params: dict[str, Any],
    timeout_seconds: int,
    api_key: Any | None,
) -> Any:
    # These market-data endpoints are public. Do not put an optional API key in
    # the curl argument list where it could be visible to local process tools.
    del api_key
    url = f"{base_url}/{rest_path}?{urlencode(params)}"
    curl = shutil.which("curl")
    if curl is None:
        raise BinanceCLIError("curl is required for Binance REST fallback")
    command = [
        curl,
        "-fsS",
        "--max-time",
        str(timeout_seconds),
        "--retry",
        "2",
        "--retry-delay",
        "1",
        "--retry-all-errors",
        "-H",
        "User-Agent: motis-binance-adapter",
    ]
    command.append(url)
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        message = completed.stderr.strip() or f"curl exited {completed.returncode}"
        raise BinanceCLIError(message[:500])
    payload = completed.stdout
    try:
        return json.loads(payload)
    except JSONDecodeError as exc:
        prefix = payload[:240].replace("\n", "\\n")
        raise BinanceCLIError(f"non-JSON REST response: {prefix}") from exc


def _request_futures_data_json(
    *,
    rest_path: str,
    params: dict[str, Any],
    timeout_seconds: int,
    api_key: Any | None,
) -> Any:
    return _request_binance_json(
        base_url=FUTURES_DATA_BASE_URL,
        rest_path=rest_path,
        params=params,
        timeout_seconds=timeout_seconds,
        api_key=api_key,
    )


def _request_futures_api_json(
    *,
    rest_path: str,
    params: dict[str, Any],
    timeout_seconds: int,
    api_key: Any | None,
) -> Any:
    return _request_binance_json(
        base_url=FUTURES_API_BASE_URL,
        rest_path=rest_path,
        params=params,
        timeout_seconds=timeout_seconds,
        api_key=api_key,
    )


@dataclass(frozen=True, slots=True)
class BinanceCLIAdapter:
    config: dict[str, Any]
    command_runner: Callable[..., str] = _default_command_runner
    adapter_id: str = "binance"

    def open_interest_statistics(
        self,
        *,
        symbol: str,
        period: str,
        limit: int,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._futures_usds_history(
            endpoint="open-interest-statistics",
            symbol=symbol,
            period=period,
            limit=limit,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

    def top_trader_account_ratio(
        self,
        *,
        symbol: str,
        period: str,
        limit: int,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._futures_usds_history(
            endpoint="top-trader-long-short-ratio-accounts",
            symbol=symbol,
            period=period,
            limit=limit,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

    def top_trader_position_ratio(
        self,
        *,
        symbol: str,
        period: str,
        limit: int,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._futures_usds_history(
            endpoint="top-trader-long-short-ratio-positions",
            symbol=symbol,
            period=period,
            limit=limit,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

    def global_account_ratio(
        self,
        *,
        symbol: str,
        period: str,
        limit: int,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._futures_usds_history(
            endpoint="long-short-ratio",
            symbol=symbol,
            period=period,
            limit=limit,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

    def taker_buy_sell_volume(
        self,
        *,
        symbol: str,
        period: str,
        limit: int,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        return self._futures_usds_history(
            endpoint="taker-buy-sell-volume",
            symbol=symbol,
            period=period,
            limit=limit,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

    def funding_rate_history(
        self,
        *,
        symbol: str,
        limit: int,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        command = [
            self._cli_path(),
            "futures-usds",
            "get-funding-rate-history",
            "--symbol",
            symbol,
            "--limit",
            str(limit),
        ]
        if start_time_ms is not None:
            command.extend(["--start-time", str(start_time_ms)])
        if end_time_ms is not None:
            command.extend(["--end-time", str(end_time_ms)])
        profile = self.config.get("profile")
        if profile:
            command.extend(["--profile", str(profile)])

        stdout = self.command_runner(
            command,
            timeout_seconds=int(self.config.get("timeout_seconds", 30)),
        )
        parsed = _parse_cli_json(stdout, label="funding-rate-history")
        if not isinstance(parsed, list):
            raise BinanceCLIError("Binance CLI funding-rate-history returned non-list JSON")
        return [dict(item) for item in parsed if isinstance(item, dict)]

    def premium_index_klines(
        self,
        *,
        symbol: str,
        interval: str,
        limit: int,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[list[Any]]:
        command = [
            self._cli_path(),
            "futures-usds",
            "premium-index-kline-data",
            "--symbol",
            symbol,
            "--interval",
            interval,
            "--limit",
            str(limit),
        ]
        if start_time_ms is not None:
            command.extend(["--start-time", str(start_time_ms)])
        if end_time_ms is not None:
            command.extend(["--end-time", str(end_time_ms)])
        profile = self.config.get("profile")
        if profile:
            command.extend(["--profile", str(profile)])

        try:
            stdout = self.command_runner(
                command,
                timeout_seconds=int(self.config.get("timeout_seconds", 60)),
            )
            parsed = _parse_cli_json(stdout, label="premium-index-kline-data")
        except BinanceCLIError as cli_error:
            return self._premium_index_rest_fallback(
                symbol=symbol,
                interval=interval,
                limit=limit,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                cli_error=cli_error,
            )
        if not isinstance(parsed, list):
            raise BinanceCLIError(
                "Binance CLI premium-index-kline-data returned non-list JSON"
            )
        return [list(item) for item in parsed if isinstance(item, list)]

    def _futures_usds_history(
        self,
        *,
        endpoint: str,
        symbol: str,
        period: str,
        limit: int,
        start_time_ms: int | None,
        end_time_ms: int | None,
    ) -> list[dict[str, Any]]:
        command = [
            self._cli_path(),
            "futures-usds",
            endpoint,
            "--symbol",
            symbol,
            "--period",
            period,
            "--limit",
            str(limit),
        ]
        if start_time_ms is not None:
            command.extend(["--start-time", str(start_time_ms)])
        if end_time_ms is not None:
            command.extend(["--end-time", str(end_time_ms)])
        profile = self.config.get("profile")
        if profile:
            command.extend(["--profile", str(profile)])
        try:
            stdout = self.command_runner(
                command,
                timeout_seconds=int(self.config.get("timeout_seconds", 30)),
            )
            parsed = _parse_cli_json(stdout, label=endpoint)
        except BinanceCLIError as cli_error:
            return self._futures_data_rest_fallback(
                endpoint=endpoint,
                symbol=symbol,
                period=period,
                limit=limit,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                cli_error=cli_error,
            )
        if not isinstance(parsed, list):
            raise BinanceCLIError(f"Binance CLI {endpoint} returned non-list JSON")
        return [dict(item) for item in parsed if isinstance(item, dict)]

    def _premium_index_rest_fallback(
        self,
        *,
        symbol: str,
        interval: str,
        limit: int,
        start_time_ms: int | None,
        end_time_ms: int | None,
        cli_error: BinanceCLIError,
    ) -> list[list[Any]]:
        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "limit": int(limit),
        }
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        timeout_seconds = int(self.config.get("http_timeout_seconds", self.config.get("timeout_seconds", 60)))
        try:
            parsed = _request_futures_api_json(
                rest_path="premiumIndexKlines",
                params=params,
                timeout_seconds=timeout_seconds,
                api_key=self.config.get("api_key") or os.environ.get("BINANCE_API_KEY"),
            )
        except BinanceCLIError as fallback_error:
            raise BinanceCLIError(
                f"{cli_error}; REST fallback for premium-index-kline-data failed: {fallback_error}"
            ) from fallback_error
        if not isinstance(parsed, list):
            raise BinanceCLIError(
                f"{cli_error}; REST fallback for premium-index-kline-data returned non-list JSON"
            )
        return [list(item) for item in parsed if isinstance(item, list)]

    def _futures_data_rest_fallback(
        self,
        *,
        endpoint: str,
        symbol: str,
        period: str,
        limit: int,
        start_time_ms: int | None,
        end_time_ms: int | None,
        cli_error: BinanceCLIError,
    ) -> list[dict[str, Any]]:
        rest_path = FUTURES_HISTORY_REST_PATHS.get(endpoint)
        if rest_path is None:
            raise cli_error
        params: dict[str, Any] = {
            "symbol": symbol,
            "period": period,
            "limit": int(limit),
        }
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        timeout_seconds = int(self.config.get("http_timeout_seconds", self.config.get("timeout_seconds", 30)))
        try:
            parsed = _request_futures_data_json(
                rest_path=rest_path,
                params=params,
                timeout_seconds=timeout_seconds,
                api_key=self.config.get("api_key") or os.environ.get("BINANCE_API_KEY"),
            )
        except BinanceCLIError as fallback_error:
            raise BinanceCLIError(
                f"{cli_error}; REST fallback for {endpoint} failed: {fallback_error}"
            ) from fallback_error
        if not isinstance(parsed, list):
            raise BinanceCLIError(
                f"{cli_error}; REST fallback for {endpoint} returned non-list JSON"
            )
        return [dict(item) for item in parsed if isinstance(item, dict)]

    def _cli_path(self) -> str:
        configured = self.config.get("cli_path")
        if configured:
            return str(configured)
        discovered = shutil.which("binance-cli")
        if discovered is None:
            raise BinanceCLIError("missing binance-cli executable")
        return discovered


def _parse_cli_json(stdout: str, *, label: str) -> Any:
    text = stdout.strip()
    try:
        return json.loads(text)
    except JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    for index, character in enumerate(text):
        if character not in "[{":
            continue
        try:
            parsed, _ = decoder.raw_decode(text[index:])
        except JSONDecodeError:
            continue
        return parsed

    prefix = text[:240].replace("\n", "\\n")
    raise BinanceCLIError(f"Binance CLI {label} returned non-JSON output: {prefix}")
