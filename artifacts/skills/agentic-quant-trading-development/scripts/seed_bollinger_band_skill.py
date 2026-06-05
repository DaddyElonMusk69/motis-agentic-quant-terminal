#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed asset-specific Bollinger Band strategy skills from the base template."
    )
    parser.add_argument("--base", required=True, help="Bollinger base template directory.")
    parser.add_argument("--asset", required=True, help="Asset symbol, e.g. BTC.")
    parser.add_argument("--out", required=True, help="Output strategy skill directory.")
    parser.add_argument(
        "--strategy-id",
        help="Optional strategy id override, e.g. btc-bollinger-band-v01.",
    )
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def prepare_output(path: Path, overwrite: bool) -> None:
    if path.exists():
        if not overwrite:
            raise SystemExit(f"Output already exists: {path}")
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    path.mkdir(parents=True, exist_ok=True)


def text_files(root: Path) -> list[Path]:
    allowed_suffixes = {".md", ".json"}
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file()
        and "__pycache__" not in path.parts
        and path.suffix in allowed_suffixes
    )


def seed_asset_text(text: str, asset: str, strategy_id: str | None) -> str:
    upper = asset.upper()
    lower = asset.lower()
    title = lower.capitalize()
    rendered = (
        text.replace("<ASSET>", upper)
        .replace("<Asset>", title)
        .replace("<asset>", lower)
    )
    if strategy_id:
        rendered = rendered.replace(f"{lower}-bollinger-band-v01", strategy_id)
    return rendered


def render_tree(base: Path, out: Path, asset: str, strategy_id: str | None) -> None:
    for source_path in text_files(base):
        rel = source_path.relative_to(base)
        out_path = out / rel
        out_path.parent.mkdir(parents=True, exist_ok=True)
        text = source_path.read_text(errors="strict")
        out_path.write_text(seed_asset_text(text, asset, strategy_id))


def main() -> int:
    args = parse_args()
    base = Path(args.base)
    out = Path(args.out)
    if not base.is_dir():
        raise SystemExit(f"Base template does not exist: {base}")
    prepare_output(out, args.overwrite)
    render_tree(base, out, args.asset, args.strategy_id)
    print(f"seeded asset skill: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
