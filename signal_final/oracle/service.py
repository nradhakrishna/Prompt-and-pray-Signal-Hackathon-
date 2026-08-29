"""Oracle orchestration at the P1/P2 verification boundary."""

from __future__ import annotations

import time
from typing import Any, Mapping

from shared.interfaces import ControlPlaneAPI
from shared.models import OracleResult, TaskStatus, WorkerResult

from .attribution import FaultCategory
from .ledger import Ledger
from .metrics import DemoMetrics
from .validation import ValidationFailure, run_test_command, validate_output


class Oracle:
    def __init__(self, control_plane: ControlPlaneAPI, *, ledger: Ledger | None = None,
                 metrics: DemoMetrics | None = None) -> None:
        self.control_plane = control_plane
        self.ledger = ledger or Ledger()
        self.metrics = metrics or DemoMetrics()
        self._results: dict[str, OracleResult] = {}

    def validate(self, worker_result: WorkerResult | Mapping[str, Any]) -> OracleResult:
        result = worker_result if isinstance(worker_result, WorkerResult) else WorkerResult(**worker_result)
        key = f"{result.task_id}:{result.attempt}"
        if key in self._results:
            self.metrics.duplicate_commits += 1
            return self._results[key]

        started = time.monotonic()
        task = self.control_plane.get_task(result.task_id)
        if task.status != TaskStatus.VERIFYING:
            raise ValueError(f"task {result.task_id!r} must be VERIFYING, not {task.status.value}")
        if task.worker_id != result.worker_id or task.attempt != result.attempt:
            return self._finish_failure(
                key, result, FaultCategory.INFRA_ERROR,
                "stale or foreign WorkerResult", started, terminal=False,
            )

        specification = dict(task.payload.get("validation") or {})
        checks: tuple[str, ...] = ()
        try:
            checks += validate_output(result.output, specification)
            command = specification.get("test_command")
            if command:
                checks += run_test_command(
                    command, cwd=specification.get("test_cwd"),
                    timeout=float(specification.get("timeout", 30)),
                )
        except ValidationFailure as exc:
            terminal = exc.category is not FaultCategory.INFRA_ERROR
            return self._finish_failure(key, result, exc.category, str(exc), started, terminal=terminal)
        except Exception as exc:
            return self._finish_failure(
                key, result, FaultCategory.INFRA_ERROR, f"oracle failure: {exc}", started,
                terminal=False,
            )

        oracle_result = OracleResult(
            result.task_id, True, worker_id=result.worker_id, attempt=result.attempt,
            checks=checks, duration=time.monotonic() - started,
        )
        self.control_plane.mark_done(result.task_id, result)
        self.ledger.settle(key, result.worker_id, True)
        self.metrics.tasks_completed += 1
        self.metrics.oracle_passes += 1
        self._results[key] = oracle_result
        return oracle_result

    def _finish_failure(self, key: str, result: WorkerResult, category: FaultCategory,
                        error: str, started: float, *, terminal: bool) -> OracleResult:
        oracle_result = OracleResult(
            result.task_id, False, category.value, error, result.worker_id, result.attempt,
            duration=time.monotonic() - started,
        )
        if terminal:
            self.control_plane.mark_failed(result.task_id, f"{category.value}: {error}")
        self.metrics.oracle_failures += 1
        if terminal:
            self.ledger.settle(key, result.worker_id, False, category)
            self._results[key] = oracle_result
        return oracle_result
