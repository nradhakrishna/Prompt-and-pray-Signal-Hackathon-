"""Async boundary between the deterministic control plane and worker runtime."""

from __future__ import annotations

from shared.models import Task, WorkerResult

from .scheduler import ControlPlane
from .task_store import InvalidTaskTransition


class AsyncWorkerControlPlane:
    """Expose Person 1 through the narrow async protocol owned by Person 2."""

    def __init__(self, control_plane: ControlPlane) -> None:
        self.control_plane = control_plane

    async def claim_task(self, worker_id: str) -> Task | None:
        return self.control_plane.claim_task(worker_id)

    async def heartbeat(self, task_id: str, worker_id: str) -> bool:
        try:
            self.control_plane.heartbeat(task_id, worker_id)
        except InvalidTaskTransition:
            return False
        return True

    async def mark_verifying(self, task_id: str, worker_id: str) -> None:
        task = self.control_plane.get_task(task_id)
        if task.worker_id != worker_id:
            raise InvalidTaskTransition(f"worker {worker_id!r} does not own task {task_id!r}")
        self.control_plane.mark_verifying(task_id)

    async def commit_result(self, result: WorkerResult) -> bool:
        self.control_plane.stage_result(result)
        return True

    async def report_failure(self, task_id: str, worker_id: str, error: str) -> None:
        task = self.control_plane.get_task(task_id)
        if task.worker_id == worker_id:
            self.control_plane.mark_failed(task_id, error)
