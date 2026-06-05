from __future__ import annotations

import argparse
import json
import os

from quant_terminal_api.repositories.runtime import RuntimeRepository


def main() -> int:
    parser = argparse.ArgumentParser(description="Canonicalize signal pools to one pool per engine/asset.")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--apply", action="store_true", help="Apply changes. Without this flag, runs in dry-run mode.")
    args = parser.parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required")

    repository = RuntimeRepository(args.database_url)
    report = repository.canonicalize_signal_pools(dry_run=not args.apply)
    print(json.dumps(report, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
