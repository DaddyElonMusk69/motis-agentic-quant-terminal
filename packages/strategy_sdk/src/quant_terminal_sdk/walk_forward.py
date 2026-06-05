from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


def _parse_days(value: str) -> int:
    if not value.endswith("d"):
        raise ValueError(f"Only day durations are supported for v1, got {value!r}")
    days = int(value[:-1])
    if days <= 0:
        raise ValueError(f"Duration must be positive, got {value!r}")
    return days


@dataclass(frozen=True, slots=True)
class WalkForwardWindow:
    template_id: str
    train_start: date
    train_end: date
    walk_forward_start: date
    walk_forward_end: date


@dataclass(frozen=True, slots=True)
class WalkForwardTemplate:
    template_id: str
    retrain_cadence: str
    train_range: str
    walk_forward_range: str
    embargo: str
    anchor: str = "rolling"

    def materialize(self, as_of: date) -> WalkForwardWindow:
        if self.anchor != "rolling":
            raise ValueError(f"Unsupported v1 walk-forward anchor: {self.anchor!r}")

        train_days = _parse_days(self.train_range)
        walk_forward_days = _parse_days(self.walk_forward_range)
        embargo_days = _parse_days(self.embargo) if self.embargo != "0d" else 0

        walk_forward_start = as_of
        walk_forward_end = walk_forward_start + timedelta(days=walk_forward_days - 1)
        train_end = walk_forward_start - timedelta(days=embargo_days + 1)
        train_start = train_end - timedelta(days=train_days - 1)

        return WalkForwardWindow(
            template_id=self.template_id,
            train_start=train_start,
            train_end=train_end,
            walk_forward_start=walk_forward_start,
            walk_forward_end=walk_forward_end,
        )
