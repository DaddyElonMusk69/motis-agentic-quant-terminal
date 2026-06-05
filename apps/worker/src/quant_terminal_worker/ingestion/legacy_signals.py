from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from quant_terminal_api.repositories.runtime import RuntimeRepository


def import_legacy_signal_sets(
    *,
    root: Path,
    repository: RuntimeRepository,
    limit: int | None = None,
) -> dict[str, Any]:
    signal_engine_id = root.name
    _register_known_engine(signal_engine_id, repository)
    imported_sets = 0
    imported_signals = 0

    for manifest_path in sorted(root.glob("*/**/manifest.json")):
        if limit is not None and imported_signals >= limit:
            break
        manifest = json.loads(manifest_path.read_text())
        if not _is_importable_signal_set_manifest(manifest_path, manifest):
            continue
        signal_set_root = manifest_path.parent
        asset = manifest["asset"]
        source_signal_set_id = manifest["signal_set_id"]
        signal_set_id = canonical_signal_set_id(signal_engine_id, asset)
        signal_set_key = build_signal_set_key(signal_engine_id, asset, signal_set_id)
        packets_root = signal_set_root / manifest.get("packets_path", "packets")
        packet_paths = sorted(packets_root.glob("*.json"))
        first_packet = _read_first_packet(packet_paths)
        actual_start_ts, actual_end_ts = _packet_time_bounds(packet_paths)
        canonical_manifest = {
            **manifest,
            "source_signal_set_id": source_signal_set_id,
            "canonical_signal_set_id": signal_set_id,
            "canonical_signal_set_key": signal_set_key,
        }

        repository.upsert_signal_set(
            {
                "signal_set_key": signal_set_key,
                "signal_set_id": signal_set_id,
                "signal_engine_id": signal_engine_id,
                "signal_engine_version": manifest.get("signal_engine_version", "unknown"),
                "asset": asset,
                "instrument": manifest.get("instrument", f"{asset}-USDT-SWAP"),
                "start_ts": actual_start_ts,
                "end_ts": actual_end_ts,
                "packet_count": len(packet_paths),
                "payload_schema": first_packet.get("schema_version", "unknown"),
                "source_path": str(signal_set_root),
                "manifest": canonical_manifest,
            }
        )
        imported_sets += 1

        for packet_path in packet_paths:
            if limit is not None and imported_signals >= limit:
                break
            packet = json.loads(packet_path.read_text())
            repository.upsert_signal(
                {
                    "signal_id": build_signal_id(
                        signal_engine_id=signal_engine_id,
                        asset=asset,
                        signal_set_id=signal_set_id,
                        packet_path=packet_path,
                    ),
                    "signal_set_key": signal_set_key,
                    "signal_engine_id": signal_engine_id,
                    "signal_engine_version": manifest.get("signal_engine_version", "unknown"),
                    "asset": asset,
                    "instrument": manifest.get("instrument", f"{asset}-USDT-SWAP"),
                    "timestamp": packet["timestamp"],
                    "data_refs": [manifest["data_manifest"]] if manifest.get("data_manifest") else [],
                    "payload_schema": packet.get("schema_version", "unknown"),
                    "payload": packet,
                }
            )
            imported_signals += 1
        repository.refresh_signal_set_coverage(signal_set_key)

    return {
        "status": "imported",
        "signal_engine_id": signal_engine_id,
        "signal_sets": imported_sets,
        "signals": imported_signals,
    }


def _is_importable_signal_set_manifest(manifest_path: Path, manifest: dict[str, Any]) -> bool:
    if not manifest.get("asset") or not manifest.get("signal_set_id"):
        return False
    packets_path = manifest.get("packets_path", "packets")
    packets_root = manifest_path.parent / packets_path
    return packets_root.exists() and any(packets_root.glob("*.json"))


def build_signal_set_key(signal_engine_id: str, asset: str, signal_set_id: str) -> str:
    return f"{signal_engine_id}:{asset}:{signal_set_id}"


def canonical_signal_set_id(signal_engine_id: str, asset: str) -> str:
    return f"{asset}-{signal_engine_id}-canonical"


def build_signal_id(
    *,
    signal_engine_id: str,
    asset: str,
    signal_set_id: str,
    packet_path: Path,
) -> str:
    return f"{signal_engine_id}:{asset}:{signal_set_id}:{packet_path.stem}"


def _read_first_packet(packet_paths: list[Path]) -> dict[str, Any]:
    if not packet_paths:
        return {}
    return json.loads(packet_paths[0].read_text())


def _packet_time_bounds(packet_paths: list[Path]) -> tuple[str | None, str | None]:
    timestamps = [json.loads(path.read_text())["timestamp"] for path in packet_paths]
    if not timestamps:
        return None, None
    return min(timestamps), max(timestamps)


def _register_known_engine(signal_engine_id: str, repository: RuntimeRepository) -> None:
    if signal_engine_id != "vegas_ema":
        repository.register_signal_engine(
            {
                "signal_engine_id": signal_engine_id,
                "name": signal_engine_id,
                "description": "Imported legacy signal engine.",
                "version": "unknown",
                "code_ref": {},
                "supported_input_data_types": ["candles"],
                "output_envelope_version": "unknown",
                "runtime_entrypoint": "",
                "live_scanner_entrypoint": None,
                "configuration_schema": {},
            }
        )
        return

    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_ema",
            "name": "Vegas EMA Tunnel",
            "description": "Legacy deterministic Vegas tunnel neutral signal engine.",
            "version": "0.1",
            "code_ref": {
                "path": "artifacts/signal_engine",
                "source": "legacy_motis_agentic_quan_trading",
                "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_base.py",
            },
            "supported_input_data_types": ["candles"],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "artifacts/signal_engine/scripts/signals/generate_training_session.py",
            "live_scanner_entrypoint": "artifacts/signal_engine/scripts/signals/scan_okx_live_signals.py",
            "configuration_schema": {
                "proximity_threshold": "decimal string",
                "vote_threshold": "integer",
                "timeframes": "array",
            },
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Import legacy replay signal packets into Postgres.")
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")

    result = import_legacy_signal_sets(
        root=args.root,
        repository=RuntimeRepository(args.database_url),
        limit=args.limit,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
