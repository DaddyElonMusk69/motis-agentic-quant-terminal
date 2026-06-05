from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil
import subprocess
from typing import Any


class OKXCLIError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SwapOrderRequest:
    inst_id: str
    side: str
    order_type: str
    size: str
    trade_mode: str
    client_order_id: str
    position_side: str | None = None
    price: str | None = None
    reduce_only: bool = False

    def __post_init__(self) -> None:
        if self.side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        if not self.client_order_id:
            raise ValueError("client_order_id is required for idempotent live orders")


@dataclass(frozen=True, slots=True)
class OKXAdapter:
    config: dict[str, Any]
    adapter_id: str = "okx"

    def readiness_blockers(self) -> list[str]:
        backend = self.config.get("backend", "okx_cli")
        if backend == "okx_cli":
            blockers: list[str] = []
            if self._cli_path() is None:
                blockers.append("missing_okx_cli")
            if self.config.get("mode", "demo") not in {"demo", "live"}:
                blockers.append("invalid_okx_mode")
            return blockers

        required = {
            "api_key": "missing_okx_api_key",
            "api_secret": "missing_okx_api_secret",
            "passphrase": "missing_okx_passphrase",
        }
        return [
            blocker
            for key, blocker in required.items()
            if not self.config.get(key)
        ]

    def build_command(self, module: str, action: str, args: list[str] | None = None) -> list[str]:
        cli_path = self._cli_path()
        if cli_path is None:
            raise OKXCLIError("missing OKX CLI executable")

        command = [cli_path]
        profile = self.config.get("profile")
        if profile:
            command.extend(["--profile", str(profile)])

        mode = self.config.get("mode", "demo")
        if mode not in {"demo", "live"}:
            raise OKXCLIError(f"invalid OKX mode: {mode}")
        command.append(f"--{mode}")
        command.append("--json")
        command.extend([module, action])
        command.extend(args or [])
        return command

    def run_json_command(
        self,
        module: str,
        action: str,
        args: list[str] | None = None,
        timeout_seconds: int = 30,
    ) -> Any:
        command = self.build_command(module, action, args)
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        if completed.returncode != 0:
            raise OKXCLIError(completed.stderr.strip() or "OKX CLI command failed")

        try:
            parsed = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise OKXCLIError("OKX CLI returned non-JSON output") from exc

        return parsed

    def market_candles(
        self,
        inst_id: str,
        *,
        bar: str,
        limit: int,
        after: str | None = None,
    ) -> dict[str, Any]:
        args = [inst_id, "--bar", bar, "--limit", str(limit)]
        if after:
            args.extend(["--after", after])
        parsed = self.run_json_command(
            "market",
            "candles",
            args,
        )
        if isinstance(parsed, list):
            return {"code": "0", "data": parsed}
        if not isinstance(parsed, dict):
            raise OKXCLIError("OKX CLI returned unsupported candle JSON")
        return parsed

    def place_swap_order(self, request: SwapOrderRequest) -> dict[str, Any]:
        args = [
            "--instId",
            request.inst_id,
            "--side",
            request.side,
            "--ordType",
            request.order_type,
            "--sz",
            request.size,
            "--tdMode",
            request.trade_mode,
            "--clOrdId",
            request.client_order_id,
        ]
        if request.position_side:
            args.extend(["--posSide", request.position_side])
        if request.price:
            args.extend(["--px", request.price])
        if request.reduce_only:
            args.append("--reduceOnly")
        parsed = self.run_json_command("swap", "place", args)
        if not isinstance(parsed, dict):
            raise OKXCLIError("OKX CLI returned JSON that was not an object")
        return parsed

    def _cli_path(self) -> str | None:
        configured = self.config.get("cli_path")
        if configured:
            path = Path(str(configured))
            if path.exists():
                return str(path)
            return shutil.which(str(configured))
        return shutil.which("okx")
