"""Deterministic DAG validation and layering for the control plane."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Iterable

from shared.models import Task


class DagValidationError(ValueError):
    """Raised when a task submission cannot be scheduled safely."""


@dataclass(frozen=True, slots=True)
class TaskDAG:
    """A validated, immutable task dependency graph.

    Cycles are rejected at the control-plane boundary.  A higher-level planner
    may collapse a semantic cycle into one task before it reaches this layer.
    """

    tasks: tuple[Task, ...]

    def __init__(self, tasks: Iterable[Task]) -> None:
        object.__setattr__(self, "tasks", tuple(tasks))
        self._validate()

    @property
    def task_ids(self) -> set[str]:
        return {task.task_id for task in self.tasks}

    @property
    def dependents(self) -> dict[str, tuple[str, ...]]:
        result: dict[str, list[str]] = defaultdict(list)
        for task in self.tasks:
            for dependency in task.dependencies:
                result[dependency].append(task.task_id)
        return {task_id: tuple(children) for task_id, children in result.items()}

    def topological_layers(self) -> list[list[str]]:
        """Return deterministic execution layers (independent tasks together)."""

        task_by_id = {task.task_id: task for task in self.tasks}
        pending = {task.task_id: len(task.dependencies) for task in self.tasks}
        children = self.dependents
        ready = sorted(task_id for task_id, degree in pending.items() if degree == 0)
        layers: list[list[str]] = []

        while ready:
            layer = ready
            layers.append(layer)
            next_ready: list[str] = []
            for task_id in layer:
                for child_id in children.get(task_id, ()):
                    pending[child_id] -= 1
                    if pending[child_id] == 0:
                        next_ready.append(child_id)
            ready = sorted(next_ready)

        if sum(map(len, layers)) != len(task_by_id):
            # The initial validation produces a clearer error in normal use;
            # keep this guard in case the implementation changes later.
            raise DagValidationError("dependency graph contains a cycle")
        return layers

    def _validate(self) -> None:
        identifiers = [task.task_id for task in self.tasks]
        duplicate_ids = sorted({item for item in identifiers if identifiers.count(item) > 1})
        if duplicate_ids:
            raise DagValidationError(f"duplicate task ids: {', '.join(duplicate_ids)}")

        known = set(identifiers)
        unknown: list[str] = []
        for task in self.tasks:
            unknown.extend(dep for dep in task.dependencies if dep not in known)
        if unknown:
            raise DagValidationError(
                "unknown dependencies: " + ", ".join(sorted(set(unknown)))
            )

        # A Kahn pass detects cycles without recursion limits or ambiguity.
        pending = {task.task_id: len(task.dependencies) for task in self.tasks}
        children = self.dependents
        ready = deque(task_id for task_id, degree in pending.items() if degree == 0)
        processed = 0
        while ready:
            task_id = ready.popleft()
            processed += 1
            for child_id in children.get(task_id, ()):
                pending[child_id] -= 1
                if pending[child_id] == 0:
                    ready.append(child_id)
        if processed != len(self.tasks):
            cyclic = sorted(task_id for task_id, degree in pending.items() if degree > 0)
            raise DagValidationError("dependency cycle involving: " + ", ".join(cyclic))
