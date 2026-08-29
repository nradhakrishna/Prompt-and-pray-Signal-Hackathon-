"""Deterministic fault attribution rules."""

from __future__ import annotations

from enum import Enum


class FaultCategory(str, Enum):
    WORKER_ERROR = "WORKER_ERROR"
    TEST_ERROR = "TEST_ERROR"
    INTEGRATION_ERROR = "INTEGRATION_ERROR"
    INFRA_ERROR = "INFRA_ERROR"


def worker_is_accountable(category: FaultCategory) -> bool:
    """Only faults caused by worker output may change worker reputation."""

    return category in {FaultCategory.WORKER_ERROR, FaultCategory.INTEGRATION_ERROR}
