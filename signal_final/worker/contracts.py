"""Worker-facing protocol built on the control plane's canonical models."""

from __future__ import annotations

from typing import Mapping, Protocol, runtime_checkable

from shared.models import Task, WorkerResult


@runtime_checkable
class ControlPlane(Protocol):
    """Person 1 adapter. Implementations may use Redis, HTTP, or an RPC client."""

    async def claim_task(self, worker_id: str) -> Task | Mapping[str, Any] | None: ...
    async def heartbeat(self, task_id: str, worker_id: str) -> bool | None: ...
    async def mark_verifying(self, task_id: str, worker_id: str) -> None: ...
    async def commit_result(self, result: WorkerResult) -> bool: ...
    async def report_failure(self, task_id: str, worker_id: str, error: str) -> None: ...
