"""Frozen data contracts exchanged between all three implementation tracks."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping


class TaskStatus(str, Enum):
    """The only task states understood by the control plane."""

    BLOCKED = "BLOCKED"
    READY = "READY"
    RUNNING = "RUNNING"
    VERIFYING = "VERIFYING"
    DONE = "DONE"
    FAILED = "FAILED"


@dataclass(slots=True)
class Task:
    """A schedulable unit of work.

    ``payload`` is intentionally opaque to the control plane.  The worker owns
    its interpretation and the oracle owns validation of the resulting output.
    """

    task_id: str
    dependencies: tuple[str, ...] = ()
    resources: tuple[str, ...] = ()
    payload: dict[str, Any] = field(default_factory=dict)
    stake: int = 0
    status: TaskStatus = TaskStatus.BLOCKED
    worker_id: str | None = None
    lease_expiry: float | None = None
    attempt: int = 0
    checkpoint_id: str | None = None
    result: dict[str, Any] | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        self.dependencies = tuple(self.dependencies)
        self.resources = tuple(self.resources)
        if not self.task_id:
            raise ValueError("task_id must be non-empty")
        if self.task_id in self.dependencies:
            raise ValueError(f"task {self.task_id!r} cannot depend on itself")
        if self.attempt < 0:
            raise ValueError("attempt cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable snapshot suitable for Redis storage."""

        return {
            "task_id": self.task_id,
            "dependencies": list(self.dependencies),
            "resources": list(self.resources),
            "payload": deepcopy(self.payload),
            "stake": self.stake,
            "status": self.status.value,
            "worker_id": self.worker_id,
            "lease_expiry": self.lease_expiry,
            "attempt": self.attempt,
            "checkpoint_id": self.checkpoint_id,
            "result": deepcopy(self.result),
            "failure_reason": self.failure_reason,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "Task":
        return cls(
            task_id=str(value["task_id"]),
            dependencies=tuple(value.get("dependencies", ())),
            resources=tuple(value.get("resources", ())),
            payload=deepcopy(dict(value.get("payload") or {})),
            stake=int(value.get("stake", 0)),
            status=TaskStatus(value.get("status", TaskStatus.BLOCKED.value)),
            worker_id=value.get("worker_id"),
            lease_expiry=value.get("lease_expiry"),
            attempt=int(value.get("attempt", 0)),
            checkpoint_id=value.get("checkpoint_id"),
            result=deepcopy(dict(value["result"])) if value.get("result") is not None else None,
            failure_reason=value.get("failure_reason"),
        )

    def snapshot(self) -> "Task":
        """Return a detached copy so callers cannot mutate persisted state."""

        return Task.from_dict(self.to_dict())


@dataclass(frozen=True, slots=True)
class WorkerResult:
    """Payload submitted by Worker Runtime after a task executes."""

    task_id: str
    worker_id: str
    attempt: int
    output: str
    checkpoint_id: str | None = None
    token_cost: int = 0
    execution_time: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "worker_id": self.worker_id,
            "attempt": self.attempt,
            "output": self.output,
            "checkpoint_id": self.checkpoint_id,
            "token_cost": self.token_cost,
            "execution_time": self.execution_time,
            "metadata": deepcopy(dict(self.metadata)),
        }


@dataclass(frozen=True, slots=True)
class OracleResult:
    """Deterministic validation outcome supplied by the Oracle track."""

    task_id: str
    passed: bool
    failure_type: str | None = None
    error: str | None = None
    worker_id: str | None = None
    attempt: int = 0
    checks: tuple[str, ...] = ()
    duration: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "passed": self.passed,
            "failure_type": self.failure_type,
            "error": self.error,
            "worker_id": self.worker_id,
            "attempt": self.attempt,
            "checks": list(self.checks),
            "duration": self.duration,
        }
