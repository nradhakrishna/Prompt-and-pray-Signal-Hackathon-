from __future__ import annotations

import asyncio
from collections import Counter
import tempfile
import unittest
from pathlib import Path

from worker.checkpoints import FileCheckpointStore
from worker.contracts import Task, WorkerResult
from worker.executors import StepExecutor
from worker.runtime import Worker, WorkerConfig


class ControlPlaneDouble:
    def __init__(self, task: Task) -> None:
        self.task = task
        self.deliver = True
        self.heartbeats = 0
        self.results: dict[str, WorkerResult] = {}
        self.commit_attempts = Counter()
        self.verifying: list[str] = []
        self.failures: list[str] = []

    async def claim_task(self, worker_id: str):
        if not self.deliver:
            return None
        self.deliver = False
        return self.task

    async def heartbeat(self, task_id: str, worker_id: str):
        self.heartbeats += 1
        return True

    async def mark_verifying(self, task_id: str):
        self.verifying.append(task_id)

    async def commit_result(self, result: WorkerResult):
        self.commit_attempts[result.task_id] += 1
        if result.task_id in self.results:
            return False
        self.results[result.task_id] = result
        return True

    async def report_failure(self, task_id: str, worker_id: str, error: str):
        self.failures.append(error)


class WorkerRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    async def test_claim_heartbeat_checkpoint_and_worker_result(self):
        cp = ControlPlaneDouble(Task("job-1", payload={"steps": ["a", "b", "c"]}))
        worker = Worker("w1", cp, StepExecutor(0.02), FileCheckpointStore(self.root),
                        WorkerConfig(heartbeat_interval=0.005))
        self.assertTrue(await worker.run_once())
        result = cp.results["job-1"]
        self.assertGreater(cp.heartbeats, 0)
        self.assertEqual(cp.verifying, ["job-1"])
        self.assertEqual(result.output, "abc")
        self.assertEqual((result.worker_id, result.token_cost), ("w1", 3))
        self.assertTrue(result.checkpoint_id)

    async def test_cancel_and_restart_resumes_latest_checkpoint(self):
        cp = ControlPlaneDouble(Task("recover", payload={"steps": list("abcdef")}))
        store = FileCheckpointStore(self.root)
        first = Worker("w1", cp, StepExecutor(0.02), store, WorkerConfig(0.005))
        running = asyncio.create_task(first.run_once())
        saved = None
        for _ in range(100):
            await asyncio.sleep(0.01)
            saved = await store.load("recover")
            if saved is not None:
                break
        running.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await running
        self.assertIsNotNone(saved)
        self.assertLess(saved.sequence, 6)

        cp.deliver = True
        second = Worker("w2", cp, StepExecutor(0), store, WorkerConfig(0.005))
        await second.run_once()
        self.assertEqual(cp.results["recover"].output, "abcdef")

    async def test_duplicate_delivery_does_not_double_commit(self):
        cp = ControlPlaneDouble(Task("duplicate", payload={"steps": ["x", "y"]}))
        worker = Worker("w1", cp, StepExecutor(0), FileCheckpointStore(self.root))
        await worker.run_once()
        cp.deliver = True
        await worker.run_once()
        self.assertEqual(len(cp.results), 1)
        self.assertEqual(cp.commit_attempts["duplicate"], 2)
        self.assertEqual(cp.results["duplicate"].output, "xy")

    async def test_final_checkpoint_replays_after_commit_ack_loss(self):
        cp = ControlPlaneDouble(Task("ack-loss", payload={"steps": ["a", "b"]}))
        original_commit = cp.commit_result
        calls = 0

        async def lose_first_ack(result):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ConnectionError("commit response lost")
            return await original_commit(result)

        cp.commit_result = lose_first_ack
        worker = Worker("w1", cp, StepExecutor(0), FileCheckpointStore(self.root))
        await worker.run_once()
        self.assertTrue(cp.failures)
        cp.deliver = True
        await worker.run_once()
        self.assertEqual(cp.results["ack-loss"].output, "ab")
