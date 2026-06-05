from __future__ import annotations

import sys
from pathlib import Path


WORKSPACE_ROOT = Path(__file__).resolve().parents[5]
SRC = WORKSPACE_ROOT / "artifacts" / "signal_engine" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from vegas.stage1b_screening import main


if __name__ == "__main__":
    raise SystemExit(main())
