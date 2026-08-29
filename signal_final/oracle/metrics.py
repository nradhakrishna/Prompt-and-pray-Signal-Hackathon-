"""Demo-oriented counters kept separate from scheduling decisions."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(slots=True)
class DemoMetrics:
    tasks_completed: int = 0
    worker_failures: int = 0
    recoveries: int = 0
    full_restart_tokens: int = 0
    resume_tokens: int = 0
    dependency_violations: int = 0
    duplicate_commits: int = 0
    oracle_passes: int = 0
    oracle_failures: int = 0

    @property
    def tokens_saved(self) -> int:
        return max(0, self.full_restart_tokens - self.resume_tokens)

    @property
    def recovery_savings_percent(self) -> float:
        if not self.full_restart_tokens:
            return 0.0
        return 100.0 * self.tokens_saved / self.full_restart_tokens

    def snapshot(self) -> dict[str, int | float | str]:
        return {**asdict(self), "tokens_saved": self.tokens_saved,
                "recovery_savings_percent": self.recovery_savings_percent,
                "final_result": "PASS" if self.oracle_failures == 0 and self.tasks_completed else "FAIL"}

    def format(self) -> str:
        values = self.snapshot()
        labels = {
            "tasks_completed": "Tasks completed", "worker_failures": "Worker failures",
            "recoveries": "Recoveries", "tokens_saved": "Tokens saved",
            "recovery_savings_percent": "Recovery savings", "dependency_violations": "Dependency violations",
            "duplicate_commits": "Duplicate commits", "final_result": "Final result",
        }
        lines = ["WORKFLOW METRICS", "-" * 38]
        for key in labels:
            value = values[key]
            if key == "recovery_savings_percent":
                value = f"{value:.1f}%"
            lines.append(f"{labels[key]:24} {value}")
        return "\n".join(lines)
