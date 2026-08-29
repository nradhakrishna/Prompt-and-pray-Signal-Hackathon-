from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from typing import Mapping

from control.scheduler import ControlPlane
from control.worker_adapter import AsyncWorkerControlPlane
from shared.models import Task, TaskStatus
from worker.checkpoints import FileCheckpointStore
from worker.executors import ExecutionOutput, StepExecutor
from worker.runtime import Worker, WorkerConfig


class FakeClock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class CrashAfterFirstCheckpoint:
    async def execute(self, task, state: Mapping | None, checkpoint) -> ExecutionOutput:
        del state
        first = str(task.payload["steps"][0])
        await checkpoint(1, {"completed": 1, "outputs": [first]})
        raise asyncio.CancelledError


class IntegratedWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def test_parallel_crash_resume_and_dependency_release(self) -> None:
        clock = FakeClock()
        plane = ControlPlane.in_memory(
            lease_seconds=10,
            verification_lease_seconds=30,
            clock=clock,
        )
        layers = plane.submit(
            [
                Task("A", payload={"steps": ["auth"]}),
                Task("B", payload={"steps": ["cart-", "resume"]}),
                Task("C", dependencies=("A", "B"), payload={"steps": ["checkout"]}),
            ]
        )
        self.assertEqual(layers, [["A", "B"], ["C"]])

        with tempfile.TemporaryDirectory() as directory:
            checkpoints = FileCheckpointStore(Path(directory))
            api = AsyncWorkerControlPlane(plane)
            worker_a = Worker("worker-a", api, StepExecutor(0), checkpoints, WorkerConfig(100))
            crashing_b = Worker("worker-b-crashed", api, CrashAfterFirstCheckpoint(), checkpoints, WorkerConfig(100))

            results = await asyncio.gather(
                worker_a.run_once(), crashing_b.run_once(), return_exceptions=True
            )
            self.assertTrue(results[0])
            self.assertIsInstance(results[1], asyncio.CancelledError)
            self.assertEqual(plane.get_task("A").status, TaskStatus.VERIFYING)
            self.assertEqual(plane.get_task("B").status, TaskStatus.RUNNING)
            self.assertEqual(plane.get_task("C").status, TaskStatus.BLOCKED)

            staged_a = plane.get_task("A").result
            self.assertIsNotNone(staged_a)
            plane.mark_done("A", staged_a)
            self.assertEqual(plane.get_task("C").status, TaskStatus.BLOCKED)

            clock.advance(10)
            self.assertEqual(plane.requeue_expired_tasks(), ["B"])
            recovering_b = Worker("worker-b-recovery", api, StepExecutor(0), checkpoints, WorkerConfig(100))
            self.assertTrue(await recovering_b.run_once())

            recovered = plane.get_task("B")
            self.assertEqual(recovered.status, TaskStatus.VERIFYING)
            self.assertEqual(recovered.attempt, 2)
            self.assertEqual(recovered.result["output"], "cart-resume")
            self.assertEqual(recovered.result["worker_id"], "worker-b-recovery")
            plane.mark_done("B", recovered.result)

            self.assertEqual(plane.get_task("C").status, TaskStatus.READY)
            claimed_c = plane.claim_task("worker-c")
            self.assertEqual(claimed_c.task_id, "C")


if __name__ == "__main__":
    unittest.main()
