"""Live agent demo: resolve a damaged-delivery customer case end to end."""

from __future__ import annotations

import argparse
import asyncio
import tempfile
from pathlib import Path
from typing import Any, Mapping

from agents import (
    AgentTaskPlanner,
    GeminiInteractionsClient,
    GroqResponsesClient,
    ModelClient,
    OpenAIResponsesClient,
    OpenAIWorkerExecutor,
)
from control.scheduler import ControlPlane
from control.worker_adapter import AsyncWorkerControlPlane
from demo import FakeClock, staged_result
from oracle import DemoMetrics, Ledger, Oracle
from shared.models import Task, TaskStatus
from worker.checkpoints import FileCheckpointStore
from worker.executors import ExecutionOutput
from worker.runtime import Worker, WorkerConfig


PROBLEM = """An online retailer received a support case: order ORD-1042 arrived with a
broken glass coffee press. The customer supplied a delivery photo and asks for a replacement
before an event in four days. Produce an evidence-based resolution plan and customer-ready
response. Do not issue refunds, place orders, or contact the customer."""


class CrashAfterModelCheckpoint:
    def __init__(self, delegate: OpenAIWorkerExecutor) -> None:
        self.delegate = delegate

    async def execute(self, task: Task, state: Mapping[str, Any] | None, checkpoint) -> ExecutionOutput:
        async def save_then_crash(sequence: int, value: Mapping[str, Any]) -> str:
            checkpoint_id = await checkpoint(sequence, value)
            if sequence == 2:
                raise asyncio.CancelledError
            return checkpoint_id
        return await self.delegate.execute(task, state, save_then_crash)


async def run_agent_demo(client: ModelClient, *, verbose: bool = True) -> dict[str, Any]:
    tasks, planner_tokens = await AgentTaskPlanner(client).plan(PROBLEM)
    clock = FakeClock()
    plane = ControlPlane.in_memory(lease_seconds=10, verification_lease_seconds=60, clock=clock)
    layers = plane.submit(tasks)
    metrics, ledger = DemoMetrics(), Ledger()
    oracle = Oracle(plane, ledger=ledger, metrics=metrics)
    api = AsyncWorkerControlPlane(plane)
    if verbose:
        print("REAL-WORLD CASE: damaged delivery resolution")
        print("Task-generation agent produced:", layers)

    with tempfile.TemporaryDirectory() as directory:
        checkpoints = FileCheckpointStore(Path(directory))
        executor = OpenAIWorkerExecutor(client)
        ready = [task for task in plane.list_tasks() if task.status is TaskStatus.READY]
        if not ready:
            raise RuntimeError("planner produced no root task")
        intended_crash_id = ready[0].task_id
        workers = [Worker(
            f"agent-worker-{index}", api,
            CrashAfterModelCheckpoint(executor) if task.task_id == intended_crash_id else executor,
            checkpoints, WorkerConfig(100),
        ) for index, task in enumerate(ready)]
        if verbose:
            print("Starting worker agents:", [task.task_id for task in ready], flush=True)
        outcomes = await asyncio.gather(*(worker.run_once() for worker in workers), return_exceptions=True)
        if any(isinstance(item, asyncio.CancelledError) for item in outcomes):
            metrics.worker_failures += 1
        failed = [task for task in plane.list_tasks() if task.status is TaskStatus.FAILED]
        if failed:
            details = "; ".join(
                f"{task.task_id}: {task.failure_reason or 'unknown error'}" for task in failed
            )
            raise RuntimeError(f"worker agent failed before the injected crash checkpoint: {details}")
        running = [task.task_id for task in plane.list_tasks() if task.status is TaskStatus.RUNNING]
        if len(running) != 1:
            states = {task.task_id: task.status.value for task in plane.list_tasks()}
            raise RuntimeError(
                f"fault injection expected one crashed task, got {running}; states={states}; "
                f"worker outcomes={outcomes!r}"
            )
        crash_id = running[0]
        if verbose:
            print(f"Injected crash after Gemini completed {crash_id}; checkpoint retained", flush=True)
        for task in plane.list_tasks():
            if task.status is TaskStatus.VERIFYING:
                oracle.validate(staged_result(plane, task.task_id))

        clock.advance(10)
        if crash_id in plane.requeue_expired_tasks():
            metrics.recoveries += 1
        if verbose:
            print(f"Recovering {crash_id} from checkpoint (no repeated Gemini call)", flush=True)
        recovery = Worker("agent-worker-recovery", api, executor, checkpoints, WorkerConfig(100))
        await recovery.run_once()
        oracle.validate(staged_result(plane, crash_id))

        while any(task.status not in (TaskStatus.DONE, TaskStatus.FAILED) for task in plane.list_tasks()):
            next_ready = [task.task_id for task in plane.list_tasks() if task.status is TaskStatus.READY]
            if verbose:
                print("Starting next worker agent:", next_ready, flush=True)
            worker = Worker("agent-worker-loop", api, executor, checkpoints, WorkerConfig(100))
            if not await worker.run_once():
                states = {task.task_id: task.status.value for task in plane.list_tasks()}
                raise RuntimeError(f"workflow stalled: {states}")
            newly_failed = [task for task in plane.list_tasks() if task.status is TaskStatus.FAILED]
            if newly_failed:
                details = "; ".join(
                    f"{task.task_id}: {task.failure_reason or 'unknown error'}" for task in newly_failed
                )
                raise RuntimeError(f"worker agent failed: {details}")
            verifying = next(task for task in plane.list_tasks() if task.status is TaskStatus.VERIFYING)
            oracle.validate(staged_result(plane, verifying.task_id))
            if verbose:
                print(f"Oracle PASS: {verifying.task_id}", flush=True)

    if verbose:
        print("Injected crash task:", crash_id)
        print("Recovery reused checkpointed model output: YES")
        print("Planner tokens:", planner_tokens)
        print(metrics.format())
    return {"plane": plane, "metrics": metrics, "ledger": ledger, "layers": layers}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", choices=("openai", "gemini", "groq"), default="openai")
    parser.add_argument("--model")
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--retries", type=int, default=2)
    args = parser.parse_args()
    if args.provider == "gemini":
        client = GeminiInteractionsClient(
            model=args.model or "gemini-3.7-flash",
            timeout=args.timeout,
            retries=args.retries,
        )
    elif args.provider == "groq":
        client = GroqResponsesClient(
            model=args.model or "openai/gpt-oss-20b",
            timeout=args.timeout,
            retries=args.retries,
        )
    else:
        client = OpenAIResponsesClient(model=args.model or "gpt-5.4-mini")
    asyncio.run(run_agent_demo(client))
