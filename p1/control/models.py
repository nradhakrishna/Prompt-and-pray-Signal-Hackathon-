"""Compatibility re-export; shared contracts live in :mod:`shared.models`."""

from shared.models import OracleResult, Task, TaskStatus, WorkerResult

__all__ = ["OracleResult", "Task", "TaskStatus", "WorkerResult"]
