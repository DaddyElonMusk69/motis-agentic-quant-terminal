from __future__ import annotations

from dataclasses import dataclass
import json
import shlex
import shutil
import subprocess
from typing import Any, Callable

from quant_terminal_worker.adapters.exchange import ExchangeAdapterError


class BinanceCLIError(ExchangeAdapterError):
    pass


CommandRunner = Callable[[list[str], Any], str]


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
        command = [
            self._cli_path(),
            "futures-usds",
            "open-interest-statistics",
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

        stdout = self.command_runner(
            command,
            timeout_seconds=int(self.config.get("timeout_seconds", 30)),
        )
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise BinanceCLIError("Binance CLI returned non-JSON output") from exc
        if not isinstance(parsed, list):
            raise BinanceCLIError("Binance CLI open-interest-statistics returned non-list JSON")
        return [dict(item) for item in parsed if isinstance(item, dict)]

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
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise BinanceCLIError("Binance CLI returned non-JSON output") from exc
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

        stdout = self.command_runner(
            command,
            timeout_seconds=int(self.config.get("timeout_seconds", 60)),
        )
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise BinanceCLIError(
                "Binance CLI premium-index-kline-data returned non-JSON output"
            ) from exc
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
        stdout = self.command_runner(
            command,
            timeout_seconds=int(self.config.get("timeout_seconds", 30)),
        )
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise BinanceCLIError(f"Binance CLI {endpoint} returned non-JSON output") from exc
        if not isinstance(parsed, list):
            raise BinanceCLIError(f"Binance CLI {endpoint} returned non-list JSON")
        return [dict(item) for item in parsed if isinstance(item, dict)]

    def _cli_path(self) -> str:
        configured = self.config.get("cli_path")
        if configured:
            return str(configured)
        discovered = shutil.which("binance-cli")
        if discovered is None:
            raise BinanceCLIError("missing binance-cli executable")
        return discovered
