"""Task executor interface and a deterministic resumable reference executor."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Protocol

from .contracts import Task

CheckpointCallback = Callable[[int, Mapping[str, Any]], Awaitable[str]]


@dataclass(frozen=True, slots=True)
class ExecutionOutput:
    output: str
    token_cost: int = 0


class TaskExecutor(Protocol):
    async def execute(self, task: Task, state: Mapping[str, Any] | None, checkpoint: CheckpointCallback) -> ExecutionOutput: ...


class StepExecutor:
    """Reference executor: processes payload['steps'] and checkpoints each step."""

    def __init__(self, step_delay: float = 0.01) -> None:
        self.step_delay = step_delay

    async def execute(self, task: Task, state: Mapping[str, Any] | None, checkpoint: CheckpointCallback) -> ExecutionOutput:
        steps = list(task.payload.get("steps", []))
        completed = int((state or {}).get("completed", 0))
        outputs = list((state or {}).get("outputs", []))
        for index in range(completed, len(steps)):
            await asyncio.sleep(self.step_delay)
            outputs.append(str(steps[index]))
            completed = index + 1
            await checkpoint(completed, {"completed": completed, "outputs": outputs})
        return ExecutionOutput(output="".join(outputs), token_cost=len(outputs))

