#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


REQUIRED_FILES = [
    "SKILL.md",
    "references/execution-parameters.md",
    "references/position-management.md",
    "references/failure-patterns.md",
]

SKILL_REQUIRED_PATTERNS = {
    "entry_or_directional_logic": r"\b(entry|direction|directional|bias|LONG|SHORT)\b",
    "progressive_loading": r"\b(progressive|references/execution-parameters\.md|references/position-management\.md)\b",
}

EXECUTION_REQUIRED_PATTERNS = {
    "sizing_or_margin": r"\b(size|sizing|margin|equity|leverage|notional|contract)\b",
    "tp_sl": r"\b(TP|take profit|SL|stop loss|stop)\b",
    "entry_orders": r"\b(entry|order|market|limit)\b",
}

POSITION_REQUIRED_PATTERNS = {
    "position_management": r"\b(position|hold|exit|reduce|pyramid|protection|repair)\b",
    "execution_source_of_truth": r"\bexecution-parameters\.md|execution parameters|setup source of truth\b",
}

FAILURE_REQUIRED_PATTERNS = {
    "failure_patterns": r"\b(failure|mistake|pattern|audit|protected|watch)\b",
}

ENTRY_GATE_REQUIRED_PATTERNS = {
    "entry_gate_section": r"\bentry gate\b",
    "enter_skip_gate": r"\bENTER\b.*\bSKIP\b|\bSKIP\b.*\bENTER\b",
    "directional_bias_not_permission": (
        r"directional bias.*not.*permission|bias alone.*not.*permission|"
        r"not.*permission.*directional bias"
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate strategy skill structure.")
    parser.add_argument("strategy_skill", nargs="?", type=Path, default=Path.cwd())
    parser.add_argument(
        "--require-entry-gate",
        action="store_true",
        help="Fail when SKILL.md lacks explicit Stage 1B/path-B Entry Gate wording.",
    )
    return parser.parse_args()


def has_pattern(text: str, pattern: str) -> bool:
    return re.search(pattern, text, flags=re.IGNORECASE) is not None


def check_patterns(root: Path, rel: str, patterns: dict[str, str], errors: list[str]) -> None:
    path = root / rel
    if not path.exists():
        return
    text = path.read_text(errors="replace")
    for name, pattern in patterns.items():
        if not has_pattern(text, pattern):
            errors.append(f"{rel}: missing expected {name} wording")


def check_entry_gate(root: Path, errors: list[str], warnings: list[str], require: bool) -> None:
    path = root / "SKILL.md"
    if not path.exists():
        return
    text = path.read_text(errors="replace")
    missing = [name for name, pattern in ENTRY_GATE_REQUIRED_PATTERNS.items() if not has_pattern(text, pattern)]
    if not missing:
        return

    message = (
        "SKILL.md: missing explicit Stage 1B/path-B Entry Gate wording "
        f"({', '.join(missing)})"
    )
    if require:
        errors.append(message)
    else:
        warnings.append(message)


def main() -> int:
    args = parse_args()
    root = args.strategy_skill
    errors: list[str] = []
    warnings: list[str] = []

    for rel in REQUIRED_FILES:
        if not (root / rel).is_file():
            errors.append(f"missing {rel}")

    check_patterns(root, "SKILL.md", SKILL_REQUIRED_PATTERNS, errors)
    check_patterns(root, "references/execution-parameters.md", EXECUTION_REQUIRED_PATTERNS, errors)
    check_patterns(root, "references/position-management.md", POSITION_REQUIRED_PATTERNS, errors)
    check_patterns(root, "references/failure-patterns.md", FAILURE_REQUIRED_PATTERNS, errors)
    check_entry_gate(root, errors, warnings, args.require_entry_gate)

    result = {
        "strategy_skill": str(root.resolve()),
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
    }
    print(json.dumps(result, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
