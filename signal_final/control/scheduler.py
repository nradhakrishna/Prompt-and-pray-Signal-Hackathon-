"""Public control-plane facade consumed by Worker Runtime and Oracle."""

from __future__ import annotations

import time
from typing import Any, Callable, Iterable, Mapping

from shared.models import Task, WorkerResult

from .dag import TaskDAG
from .task_store import InMemoryTaskStore, RedisTaskStore, TaskStore


class ControlPlane:
    """DAG scheduler with leases, heartbeats, and dependency release.

    Worker Runtime should use only this class (or the ``ControlPlaneAPI``
    protocol) and must never mutate store state directly.  The facade keeps a
    Redis deployment and a local/test deployment API-compatible.
    """

    def __init__(self, store: TaskStore, *, clock: Callable[[], float] = time.time):
        self._store = store
        self._clock = clock

    @classmethod
    def in_memory(
        cls,
        *,
        lease_seconds: float = 30.0,
        verification_lease_seconds: float = 30.0,
        clock: Callable[[], float] = time.time,
    ) -> "ControlPlane":
        return cls(
            InMemoryTaskStore(
                lease_seconds=lease_seconds,
                verification_lease_seconds=verification_lease_seconds,
            ),
            clock=clock,
        )

    @classmethod
    def redis(
        cls,
        *,
        redis_url: str = "redis://localhost:6379/0",
        namespace: str = "control_plane",
        lease_seconds: float = 30.0,
        verification_lease_seconds: float = 30.0,
        clock: Callable[[], float] = time.time,
    ) -> "ControlPlane":
        return cls(
            RedisTaskStore(
                redis_url=redis_url,
                namespace=namespace,
                lease_seconds=lease_seconds,
                verification_lease_seconds=verification_lease_seconds,
            ),
            clock=clock,
        )

    def submit(self, tasks: Iterable[Task]) -> list[list[str]]:
        """Validate, persist, and return the deterministic DAG execution layers."""

        submitted = list(tasks)
        dag = TaskDAG(submitted)
        self._store.add_tasks(submitted)
        return dag.topological_layers()

    def claim_task(self, worker_id: str) -> Task | None:
        """Atomically claim the next unblocked, resource-compatible task."""

        return self._store.claim_next(worker_id, self._clock())

    def heartbeat(self, task_id: str, worker_id: str) -> Task:
        """Extend the lease held by ``worker_id`` for a running task."""

        return self._store.heartbeat(task_id, worker_id, self._clock())

    def mark_verifying(self, task_id: str) -> Task:
        """Move a worker-owned task into the Oracle validation stage."""

        return self._store.mark_verifying(task_id, self._clock())

    def stage_result(self, result: WorkerResult) -> Task:
        """Store a worker result while leaving final acceptance to the Oracle."""

        return self._store.stage_result(result, self._clock())

    def mark_done(self, task_id: str, result: Mapping[str, Any] | WorkerResult) -> Task:
        """Commit the first successful result and release satisfied dependents."""

        return self._store.mark_done(task_id, result, self._clock())

    def mark_failed(self, task_id: str, reason: str) -> Task:
        """Fail a task terminally; downstream tasks remain blocked."""

        return self._store.mark_failed(task_id, reason, self._clock())

    def requeue_expired_tasks(self) -> list[str]:
        """Return RUNNING or VERIFYING work with an expired lease to READY."""

        return self._store.requeue_expired(self._clock())

    def get_task(self, task_id: str) -> Task:
        """Read a detached task snapshot for observability or integration tests."""

        return self._store.get_task(task_id)

    def list_tasks(self) -> list[Task]:
        """Read all task snapshots in deterministic task-id order."""

        return self._store.list_tasks()
