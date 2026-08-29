"""Stable interface imported by Worker Runtime and Oracle implementations."""

from __future__ import annotations

from typing import Any, Mapping, Protocol

from .models import Task, WorkerResult


class ControlPlaneAPI(Protocol):
    """The complete public surface exposed by Person 1's subsystem."""

    def claim_task(self, worker_id: str) -> Task | None:
        """Atomically claim one eligible task, or return ``None`` when idle."""

    def heartbeat(self, task_id: str, worker_id: str) -> Task:
        """Extend a running task's lease; reject stale or foreign workers."""

    def mark_verifying(self, task_id: str) -> Task:
        """Hand a completed worker attempt to the Oracle."""

    def mark_done(self, task_id: str, result: Mapping[str, Any] | WorkerResult) -> Task:
        """Commit a successful result and release newly unblocked dependents."""

    def mark_failed(self, task_id: str, reason: str) -> Task:
        """Record a terminal failure and retain blocked downstream work."""

    def requeue_expired_tasks(self) -> list[str]:
        """Return expired RUNNING/VERIFYING tasks to the ready queue."""
