"""Full Auth + Cart -> Checkout -> Integration Person 1/2/3 demo."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import Any, Mapping

from control.scheduler import ControlPlane
from control.worker_adapter import AsyncWorkerControlPlane
from oracle import DemoMetrics, Ledger, Oracle
from shared.models import Task, WorkerResult
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
    async def execute(self, task: Task, state: Mapping[str, Any] | None, checkpoint) -> ExecutionOutput:
        del state
        first = str(task.payload["steps"][0])
        await checkpoint(1, {"completed": 1, "outputs": [first]})
        raise asyncio.CancelledError


def staged_result(plane: ControlPlane, task_id: str) -> WorkerResult:
    value = plane.get_task(task_id).result
    if value is None:
        raise RuntimeError(f"task {task_id} has no staged WorkerResult")
    return WorkerResult(**value)


async def run_checkout_demo(*, verbose: bool = True) -> dict[str, Any]:
    clock = FakeClock()
    plane = ControlPlane.in_memory(lease_seconds=10, verification_lease_seconds=30, clock=clock)
    metrics, ledger = DemoMetrics(), Ledger()
    oracle = Oracle(plane, ledger=ledger, metrics=metrics)
    layers = plane.submit([
        Task("Auth", payload={"steps": ["auth"], "validation": {"expected_output": "auth"}}),
        Task("Cart", payload={"steps": ["cart-", "resume"], "validation": {"expected_output": "cart-resume"}}),
        Task("Checkout", dependencies=("Auth", "Cart"), payload={"steps": ["checkout"], "validation": {"expected_output": "checkout"}}),
        Task("Integration", dependencies=("Checkout",), payload={"steps": ["PASS"], "validation": {"expected_output": "PASS"}}),
    ])
    if verbose:
        print("DAG: Auth + Cart -> Checkout -> Integration")
        print("Execution layers:", layers)

    with tempfile.TemporaryDirectory() as directory:
        checkpoints = FileCheckpointStore(Path(directory))
        api = AsyncWorkerControlPlane(plane)
        auth = Worker("worker-auth", api, StepExecutor(0), checkpoints, WorkerConfig(100))
        cart_crash = Worker("worker-cart-crashed", api, CrashAfterFirstCheckpoint(), checkpoints, WorkerConfig(100))
        outcomes = await asyncio.gather(auth.run_once(), cart_crash.run_once(), return_exceptions=True)
        if isinstance(outcomes[1], asyncio.CancelledError):
            metrics.worker_failures += 1
        oracle.validate(staged_result(plane, "Auth"))
        if verbose:
            print("Parallel start: Auth verified; Cart worker crashed after checkpoint")
            print("Checkout remains:", plane.get_task("Checkout").status.value)

        clock.advance(10)
        expired = plane.requeue_expired_tasks()
        if "Cart" in expired:
            metrics.recoveries += 1
            metrics.full_restart_tokens += 6000
            metrics.resume_tokens += 1000
        recovery = Worker("worker-cart-recovery", api, StepExecutor(0), checkpoints, WorkerConfig(100))
        await recovery.run_once()
        oracle.validate(staged_result(plane, "Cart"))
        if verbose:
            print("Cart resumed from checkpoint; Checkout:", plane.get_task("Checkout").status.value)

        checkout = Worker("worker-checkout", api, StepExecutor(0), checkpoints, WorkerConfig(100))
        await checkout.run_once()
        oracle.validate(staged_result(plane, "Checkout"))
        integration = Worker("worker-integration", api, StepExecutor(0), checkpoints, WorkerConfig(100))
        await integration.run_once()
        final = oracle.validate(staged_result(plane, "Integration"))
        if verbose:
            print("Oracle final:", "PASS" if final.passed else "FAIL")
            print(metrics.format())
    return {"plane": plane, "metrics": metrics, "ledger": ledger, "final": final}


if __name__ == "__main__":
    asyncio.run(run_checkout_demo())
