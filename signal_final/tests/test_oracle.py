from __future__ import annotations

import sys
import unittest

from control.scheduler import ControlPlane
from oracle import DemoMetrics, FaultCategory, Ledger, Oracle
from shared.models import Task, TaskStatus, WorkerResult


def verifying_task(task_id: str, validation: dict, output: str = "ok"):
    plane = ControlPlane.in_memory()
    plane.submit([Task(task_id, payload={"validation": validation})])
    claimed = plane.claim_task("worker-1")
    assert claimed is not None
    plane.mark_verifying(task_id)
    result = WorkerResult(task_id, "worker-1", claimed.attempt, output, token_cost=3)
    plane.stage_result(result)
    return plane, result


class OracleTests(unittest.TestCase):
    def test_pass_marks_done_rewards_once_and_releases_dependency(self) -> None:
        plane = ControlPlane.in_memory()
        plane.submit([Task("A", payload={"validation": {"expected_output": "auth"}}), Task("B", dependencies=("A",))])
        task = plane.claim_task("worker-1")
        result = WorkerResult("A", "worker-1", task.attempt, "auth")
        plane.mark_verifying("A")
        plane.stage_result(result)
        ledger, metrics = Ledger(), DemoMetrics()
        oracle = Oracle(plane, ledger=ledger, metrics=metrics)
        accepted = oracle.validate(result)
        duplicate = oracle.validate(result)
        self.assertTrue(accepted.passed)
        self.assertEqual(accepted, duplicate)
        self.assertEqual(plane.get_task("A").status, TaskStatus.DONE)
        self.assertEqual(plane.get_task("B").status, TaskStatus.READY)
        self.assertEqual(ledger.balance("worker-1"), 105)
        self.assertEqual(len(ledger.entries), 1)
        self.assertEqual(metrics.tasks_completed, 1)
        self.assertEqual(metrics.duplicate_commits, 1)

    def test_worker_error_is_slashed(self) -> None:
        plane, result = verifying_task("bad-worker", {"expected_output": "right"}, "wrong")
        ledger = Ledger()
        oracle_result = Oracle(plane, ledger=ledger).validate(result)
        self.assertEqual(oracle_result.failure_type, FaultCategory.WORKER_ERROR.value)
        self.assertEqual(ledger.balance("worker-1"), 90)
        self.assertEqual(plane.get_task("bad-worker").status, TaskStatus.FAILED)

    def test_bad_test_definition_does_not_slash_worker(self) -> None:
        plane, result = verifying_task("bad-test", {"test_command": "pytest"})
        ledger = Ledger()
        oracle_result = Oracle(plane, ledger=ledger).validate(result)
        self.assertEqual(oracle_result.failure_type, FaultCategory.TEST_ERROR.value)
        self.assertEqual(ledger.balance("worker-1"), 100)

    def test_integration_failure_is_attributed_and_slashed(self) -> None:
        plane, result = verifying_task("integration", {"test_command": [sys.executable, "-c", "raise SystemExit(2)"]})
        ledger = Ledger()
        oracle_result = Oracle(plane, ledger=ledger).validate(result)
        self.assertEqual(oracle_result.failure_type, FaultCategory.INTEGRATION_ERROR.value)
        self.assertEqual(ledger.balance("worker-1"), 90)

    def test_infrastructure_fault_is_retryable_and_neutral(self) -> None:
        plane, result = verifying_task("infra", {"test_command": ["definitely-not-a-real-executable-987"]})
        ledger = Ledger()
        oracle_result = Oracle(plane, ledger=ledger).validate(result)
        self.assertEqual(oracle_result.failure_type, FaultCategory.INFRA_ERROR.value)
        self.assertEqual(ledger.balance("worker-1"), 100)
        self.assertEqual(plane.get_task("infra").status, TaskStatus.VERIFYING)


if __name__ == "__main__":
    unittest.main()
