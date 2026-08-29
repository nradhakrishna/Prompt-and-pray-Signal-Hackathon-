"""Worker loop, lease heartbeat, recovery, and idempotent result delivery."""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from dataclasses import dataclass
from typing import Any, Mapping

from .checkpoints import FileCheckpointStore
from .contracts import ControlPlane, Task, WorkerResult
from .executors import TaskExecutor

log = logging.getLogger(__name__)


class LeaseLost(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    heartbeat_interval: float = 5.0
    idle_interval: float = 1.0


class Worker:
    def __init__(self, worker_id: str, control_plane: ControlPlane, executor: TaskExecutor,
                 checkpoints: FileCheckpointStore, config: WorkerConfig | None = None) -> None:
        self.worker_id = worker_id
        self.control_plane = control_plane
        self.executor = executor
        self.checkpoints = checkpoints
        self.config = config or WorkerConfig()
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            worked = await self.run_once()
            if not worked:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self.config.idle_interval)
                except TimeoutError:
                    pass

    async def run_once(self) -> bool:
        raw = await self.control_plane.claim_task(self.worker_id)
        if raw is None:
            return False
        task = raw if isinstance(raw, Task) else Task.from_dict(raw)
        started = time.monotonic()
        heartbeat_error: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        heartbeat_task = asyncio.create_task(self._heartbeat(task, heartbeat_error))
        execution: asyncio.Task[Any] | None = None
        try:
            checkpoint = await self.checkpoints.load(task.task_id)
            state = checkpoint.state if checkpoint and checkpoint.attempt <= task.attempt else None
            latest_id = checkpoint.checkpoint_id if checkpoint else task.checkpoint_id or ""

            if checkpoint and state and state.get("_worker_final"):
                result = WorkerResult(
                    task.task_id, self.worker_id, task.attempt, str(state["output"]),
                    checkpoint.checkpoint_id, int(state.get("token_cost", 0)),
                    float(state.get("execution_time", 0.0)),
                )
                await self._mark_verifying(task)
                await self.control_plane.commit_result(result)
                return True

            async def save(sequence: int, value: Mapping[str, Any]) -> str:
                nonlocal latest_id
                saved = await self.checkpoints.save(task.task_id, task.attempt, sequence, value)
                latest_id = saved.checkpoint_id
                return latest_id

            execution = asyncio.create_task(self.executor.execute(task, state, save))
            done, _ = await asyncio.wait({execution, heartbeat_error}, return_when=asyncio.FIRST_COMPLETED)
            if heartbeat_error in done:
                execution.cancel()
                await asyncio.gather(execution, return_exceptions=True)
                raise heartbeat_error.exception() or LeaseLost(task.task_id)
            output = await execution
            elapsed = time.monotonic() - started
            final = await self.checkpoints.save(
                task.task_id, task.attempt, 2**63 - 1,
                {"_worker_final": True, "output": output.output,
                 "token_cost": output.token_cost, "execution_time": elapsed},
            )
            latest_id = final.checkpoint_id
            result = WorkerResult(task.task_id, self.worker_id, task.attempt, output.output,
                                  latest_id, output.token_cost, elapsed)
            await self._mark_verifying(task)
            await self.control_plane.commit_result(result)
            return True
        except asyncio.CancelledError:
            raise
        except LeaseLost:
            log.warning("lease lost for task %s; checkpoint retained", task.task_id)
            return True
        except Exception as exc:
            await self.control_plane.report_failure(task.task_id, self.worker_id, repr(exc))
            return True
        finally:
            if execution is not None and not execution.done():
                execution.cancel()
                await asyncio.gather(execution, return_exceptions=True)
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)

    async def _heartbeat(self, task: Task, failure: asyncio.Future[Any]) -> None:
        try:
            while True:
                await asyncio.sleep(self.config.heartbeat_interval)
                accepted = await self.control_plane.heartbeat(task.task_id, self.worker_id)
                if accepted is False:
                    if not failure.done():
                        failure.set_exception(LeaseLost(f"lease lost for {task.task_id}"))
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not failure.done():
                failure.set_exception(LeaseLost(f"heartbeat failed: {exc!r}"))

    async def _mark_verifying(self, task: Task) -> None:
        """Accept the agreed 1-arg API plus a safer worker-aware compatibility form."""
        method = self.control_plane.mark_verifying
        try:
            count = len(inspect.signature(method).parameters)
        except (TypeError, ValueError):
            count = 2
        if count == 1:
            await method(task.task_id)  # type: ignore[call-arg]
        else:
            await method(task.task_id, self.worker_id)
