from __future__ import annotations

import unittest

from control.dag import DagValidationError, TaskDAG
from control.scheduler import ControlPlane
from control.task_store import InvalidTaskTransition
from shared.models import Task, TaskStatus, WorkerResult


class FakeClock:
    def __init__(self, initial: float = 100.0) -> None:
        self.value = initial

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class ControlPlaneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.plane = ControlPlane.in_memory(lease_seconds=10, clock=self.clock)

    def test_a_and_b_claim_in_parallel_and_c_waits_for_both(self) -> None:
        layers = self.plane.submit(
            [Task("A"), Task("B"), Task("C", dependencies=("A", "B"))]
        )
        self.assertEqual(layers, [["A", "B"], ["C"]])
        self.assertEqual(self.plane.get_task("C").status, TaskStatus.BLOCKED)

        first = self.plane.claim_task("worker-1")
        second = self.plane.claim_task("worker-2")
        self.assertEqual({first.task_id, second.task_id}, {"A", "B"})
        self.assertIsNone(self.plane.claim_task("worker-3"))

        self.plane.mark_done("A", {"output": "A"})
        self.assertEqual(self.plane.get_task("C").status, TaskStatus.BLOCKED)
        self.plane.mark_done("B", {"output": "B"})
        self.assertEqual(self.plane.get_task("C").status, TaskStatus.READY)
        self.assertEqual(self.plane.claim_task("worker-3").task_id, "C")

    def test_heartbeat_extends_only_the_current_live_lease(self) -> None:
        self.plane.submit([Task("A")])
        claimed = self.plane.claim_task("worker-1")
        self.assertEqual(claimed.lease_expiry, 110.0)
        self.clock.advance(8)
        extended = self.plane.heartbeat("A", "worker-1")
        self.assertEqual(extended.lease_expiry, 118.0)
        with self.assertRaises(InvalidTaskTransition):
            self.plane.heartbeat("A", "worker-2")

    def test_expired_lease_requeues_and_next_claim_increments_attempt(self) -> None:
        self.plane.submit([Task("A")])
        first = self.plane.claim_task("worker-that-crashes")
        self.assertEqual(first.attempt, 1)
        self.clock.advance(10)

        self.assertEqual(self.plane.requeue_expired_tasks(), ["A"])
        requeued = self.plane.get_task("A")
        self.assertEqual(requeued.status, TaskStatus.READY)
        self.assertIsNone(requeued.worker_id)

        recovered = self.plane.claim_task("worker-2")
        self.assertEqual(recovered.worker_id, "worker-2")
        self.assertEqual(recovered.attempt, 2)

    def test_done_tasks_are_never_redispatched_and_completion_is_idempotent(self) -> None:
        self.plane.submit([Task("A")])
        claimed = self.plane.claim_task("worker-1")
        with self.assertRaises(InvalidTaskTransition):
            self.plane.mark_done(
                "A", WorkerResult("A", "worker-2", claimed.attempt, "foreign completion")
            )
        result = WorkerResult(
            task_id="A",
            worker_id="worker-1",
            attempt=claimed.attempt,
            output="complete",
        )
        done = self.plane.mark_done("A", result)
        self.assertEqual(done.status, TaskStatus.DONE)
        second_done = self.plane.mark_done("A", {"output": "do not overwrite"})
        self.assertEqual(second_done.result["output"], "complete")
        self.assertIsNone(self.plane.claim_task("worker-2"))

    def test_stale_worker_result_is_rejected_after_recovery(self) -> None:
        self.plane.submit([Task("A")])
        original = self.plane.claim_task("worker-1")
        self.clock.advance(11)
        self.plane.requeue_expired_tasks()
        recovered = self.plane.claim_task("worker-2")
        with self.assertRaises(InvalidTaskTransition):
            self.plane.mark_done(
                "A",
                WorkerResult("A", "worker-1", original.attempt, "late result"),
            )
        self.plane.mark_done(
            "A", WorkerResult("A", "worker-2", recovered.attempt, "current result")
        )

    def test_resource_overlap_serializes_independent_work(self) -> None:
        self.plane.submit(
            [Task("A", resources=("src/auth.py",)), Task("B", resources=("src/auth.py",))]
        )
        self.assertEqual(self.plane.claim_task("worker-1").task_id, "A")
        self.assertIsNone(self.plane.claim_task("worker-2"))
        self.plane.mark_done("A", {"output": "done"})
        self.assertEqual(self.plane.claim_task("worker-2").task_id, "B")

    def test_cycles_and_unknown_dependencies_are_rejected_before_storage(self) -> None:
        with self.assertRaises(DagValidationError):
            self.plane.submit([Task("A", dependencies=("missing",))])
        with self.assertRaises(DagValidationError):
            TaskDAG([Task("A", dependencies=("B",)), Task("B", dependencies=("A",))])


if __name__ == "__main__":
    unittest.main()
