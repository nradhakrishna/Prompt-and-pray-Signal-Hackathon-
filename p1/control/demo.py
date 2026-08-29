"""Small executable proof of DAG release and lease-based recovery.

Run with ``python -m control.demo`` from the repository root.
"""

from __future__ import annotations

from shared.models import Task, TaskStatus

from .scheduler import ControlPlane


class DemoClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def main() -> None:
    clock = DemoClock()
    plane = ControlPlane.in_memory(lease_seconds=5, clock=clock)
    layers = plane.submit(
        [
            Task("A", payload={"work": "first parallel task"}),
            Task("B", payload={"work": "second parallel task"}),
            Task("C", dependencies=("A", "B")),
        ]
    )
    print("DAG layers:", layers)

    a = plane.claim_task("worker-1")
    b = plane.claim_task("worker-2")
    print("Parallel claims:", a.task_id if a else None, b.task_id if b else None)
    print("C is blocked:", plane.get_task("C").status is TaskStatus.BLOCKED)

    plane.mark_done("A", {"output": "A complete"})
    print("C still blocked after A:", plane.get_task("C").status is TaskStatus.BLOCKED)
    plane.mark_done("B", {"output": "B complete"})
    c = plane.claim_task("worker-3")
    print("C released after A+B:", c.task_id if c else None)
    plane.mark_done("C", {"output": "C complete"})
    print("DONE tasks redispatched:", plane.claim_task("worker-4") is not None)

    plane.submit([Task("recovery")])
    lost = plane.claim_task("worker-that-crashes")
    assert lost is not None
    clock.advance(6)
    print("Expired tasks requeued:", plane.requeue_expired_tasks())
    recovered = plane.claim_task("recovery-worker")
    print("Recovery claim:", (recovered.task_id, recovered.attempt) if recovered else None)


if __name__ == "__main__":
    main()
