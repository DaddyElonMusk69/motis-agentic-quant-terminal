#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path


BASE_REPLACEMENTS = (
    ("BTC", "<ASSET>"),
    ("Btc", "<Asset>"),
    ("btc", "<asset>"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mechanically build or instantiate Vegas EMA strategy skills."
    )
    subparsers = parser.add_subparsers(dest="mode", required=True)

    build_base = subparsers.add_parser(
        "build-base",
        help="Build a placeholder Vegas EMA base from BTC v0.16 by exact token replacement.",
    )
    build_base.add_argument("--source", required=True, help="Source BTC strategy skill directory.")
    build_base.add_argument("--out", required=True, help="Output base template directory.")
    build_base.add_argument("--overwrite", action="store_true")

    seed_asset = subparsers.add_parser(
        "seed-asset",
        help="Seed an asset strategy from the Vegas EMA base by exact placeholder replacement.",
    )
    seed_asset.add_argument("--base", required=True, help="Vegas EMA base template directory.")
    seed_asset.add_argument("--asset", required=True, help="Asset symbol, e.g. WIF.")
    seed_asset.add_argument("--out", required=True, help="Output strategy skill directory.")
    seed_asset.add_argument(
        "--strategy-id",
        help="Optional strategy id override, e.g. wif-vegas-tunnel-v00.",
    )
    seed_asset.add_argument("--overwrite", action="store_true")

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


def render_tree(source: Path, out: Path, transform) -> None:
    for source_path in text_files(source):
        rel = source_path.relative_to(source)
        out_path = out / rel
        out_path.parent.mkdir(parents=True, exist_ok=True)
        text = source_path.read_text(errors="strict")
        out_path.write_text(transform(text))


def to_base(text: str) -> str:
    rendered = text
    for old, new in BASE_REPLACEMENTS:
        rendered = rendered.replace(old, new)
    return rendered


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
        default_strategy_id = f"{lower}-vegas-tunnel-v01"
        rendered = rendered.replace(default_strategy_id, strategy_id)
    return rendered


def main() -> int:
    args = parse_args()
    if args.mode == "build-base":
        source = Path(args.source)
        out = Path(args.out)
        if not source.is_dir():
            raise SystemExit(f"Source strategy skill does not exist: {source}")
        prepare_output(out, args.overwrite)
        render_tree(source, out, to_base)
        print(f"built base template: {out}")
        return 0

    if args.mode == "seed-asset":
        base = Path(args.base)
        out = Path(args.out)
        if not base.is_dir():
            raise SystemExit(f"Base template does not exist: {base}")
        prepare_output(out, args.overwrite)
        render_tree(base, out, lambda text: seed_asset_text(text, args.asset, args.strategy_id))
        print(f"seeded asset skill: {out}")
        return 0

    raise SystemExit(f"Unsupported mode: {args.mode}")


if __name__ == "__main__":
    raise SystemExit(main())
