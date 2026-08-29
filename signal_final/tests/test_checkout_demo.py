from __future__ import annotations

import unittest

from demo import run_checkout_demo
from oracle import DemoMetrics, Ledger
from shared.models import TaskStatus


class CheckoutDemoTests(unittest.IsolatedAsyncioTestCase):
    async def test_checkout_workflow_recovers_and_passes(self) -> None:
        result = await run_checkout_demo(verbose=False)
        plane = result["plane"]
        metrics: DemoMetrics = result["metrics"]
        ledger: Ledger = result["ledger"]
        self.assertTrue(all(task.status is TaskStatus.DONE for task in plane.list_tasks()))
        self.assertEqual(metrics.tasks_completed, 4)
        self.assertEqual(metrics.worker_failures, 1)
        self.assertEqual(metrics.recoveries, 1)
        self.assertEqual(metrics.tokens_saved, 5000)
        self.assertEqual(metrics.dependency_violations, 0)
        self.assertEqual(metrics.duplicate_commits, 0)
        self.assertEqual(metrics.snapshot()["final_result"], "PASS")
        self.assertGreater(ledger.balance("worker-cart-recovery"), 100)


if __name__ == "__main__":
    unittest.main()
