from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True, slots=True)
class AgentTaskBundle:
    task_id: str
    cycle_id: str
    stage: str
    strategy_id: str
    strategy_version: str
    allowed_context_paths: list[str] = field(default_factory=list)
    forbidden_context_paths: list[str] = field(default_factory=list)

    def render_prompt(self, repo_root: Path) -> str:
        allowed = "\n".join(f"- {path}" for path in self.allowed_context_paths)
        forbidden_count = len(self.forbidden_context_paths)

        return "\n".join(
            [
                "You are working in this deterministic quant terminal repo.",
                "",
                f"Repository root: {repo_root}",
                f"Agent task: {self.task_id}",
                f"Cycle: {self.cycle_id}",
                f"Stage: {self.stage}",
                f"Strategy: {self.strategy_id}@{self.strategy_version}",
                "",
                "Read these scoped context files:",
                allowed,
                "",
                "Do not inspect forbidden walk-forward data.",
                f"Forbidden context paths are tracked by the platform ({forbidden_count} paths).",
                "",
                "Write an audit note, update only the strategy module if needed, add tests, "
                "and leave deterministic scoring to the platform.",
            ]
        )
