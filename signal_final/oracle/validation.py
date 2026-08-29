"""Deterministic output checks and isolated test-command execution."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .attribution import FaultCategory


@dataclass(frozen=True, slots=True)
class ValidationFailure(Exception):
    category: FaultCategory
    message: str

    def __str__(self) -> str:
        return self.message


def validate_output(output: str, specification: Mapping[str, Any]) -> tuple[str, ...]:
    """Apply declared, reproducible checks without model judgment."""

    checks: list[str] = []
    if specification.get("non_empty", True) and not output.strip():
        raise ValidationFailure(FaultCategory.WORKER_ERROR, "worker output is empty")
    checks.append("non-empty output")

    expected = specification.get("expected_output")
    if expected is not None and output != str(expected):
        raise ValidationFailure(
            FaultCategory.WORKER_ERROR,
            f"expected output {expected!r}, received {output!r}",
        )
    if expected is not None:
        checks.append("exact output")

    for value in specification.get("required_substrings", ()): 
        if str(value) not in output:
            raise ValidationFailure(
                FaultCategory.WORKER_ERROR, f"required value {value!r} is missing"
            )
        checks.append(f"contains {value!r}")
    return tuple(checks)


def run_test_command(
    command: Sequence[str], *, cwd: str | Path | None = None, timeout: float = 30.0
) -> tuple[str, ...]:
    """Run an allowlisted argument vector (never a shell string)."""

    if not command or isinstance(command, (str, bytes)):
        raise ValidationFailure(FaultCategory.TEST_ERROR, "test_command must be an argument list")
    try:
        completed = subprocess.run(
            list(command), cwd=cwd, text=True, capture_output=True,
            timeout=timeout, check=False, shell=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise ValidationFailure(FaultCategory.INFRA_ERROR, f"test timed out: {exc}") from exc
    except OSError as exc:
        raise ValidationFailure(FaultCategory.INFRA_ERROR, f"test runner unavailable: {exc}") from exc
    if completed.returncode:
        details = (completed.stderr or completed.stdout).strip()
        raise ValidationFailure(
            FaultCategory.INTEGRATION_ERROR,
            f"test command failed ({completed.returncode}): {details[-1000:]}",
        )
    return ("test command passed",)
