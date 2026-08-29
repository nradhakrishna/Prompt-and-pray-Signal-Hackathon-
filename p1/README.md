# Distributed Task Control Plane (Person 1)

This package implements the deterministic control plane for the three-person
distributed-systems build.  It owns DAG validation, ready-task scheduling,
leases, heartbeats, expired-work recovery, resource exclusion, and dependent
task release.  It deliberately does **not** interpret task payloads, execute
work, checkpoint work, validate results, or apply token-economy rules.

## Integration contract

The frozen shared objects are in `shared/models.py`; Worker Runtime and Oracle
should import `ControlPlaneAPI` from `shared/interfaces.py` and depend only on:

```python
claim_task(worker_id) -> Task | None
heartbeat(task_id, worker_id) -> Task
mark_verifying(task_id) -> Task
mark_done(task_id, result) -> Task
mark_failed(task_id, reason) -> Task
requeue_expired_tasks() -> list[str]
```

`mark_done` is idempotent: the first terminal result wins.  Workers should pass
a `WorkerResult`, including its claim `attempt`, so a late result from a
reassigned worker cannot overwrite current work.

The control plane guarantees that:

1. A worker never claims a `BLOCKED`, `DONE`, or `FAILED` task.
2. Every `RUNNING` or `VERIFYING` task has a finite lease.
3. An expired lease returns the task to `READY`; the next claim increments its
   attempt.
4. A dependency releases only after **all** of its dependencies are `DONE`.
5. Tasks with overlapping declared resources are not claimed concurrently.

## Local proof

No package installation or service is required for the local implementation:

```bash
python3 -m unittest discover -v
python3 -m control.demo
```

The test suite proves that A and B are claimed in parallel, C is held until A
and B are done, expired leases are requeued, stale completions are rejected,
and `DONE` work is never dispatched again.

## Redis deployment

The production store is `RedisTaskStore`; it stores task snapshots under
`control_plane:task:<task_id>` and uses `control_plane:ready_queue`.  Claims,
heartbeats, completions, failure transitions, and requeue scans use Redis Lua
scripts, making each individual state transition atomic across schedulers.

Start Redis and install the optional client package:

```bash
docker compose up -d redis
python3 -m pip install -r requirements.txt
```

Then construct the control plane with:

```python
from control.scheduler import ControlPlane
from shared.models import Task

plane = ControlPlane.redis(namespace="demo")
plane.submit([Task("A"), Task("B"), Task("C", dependencies=("A", "B"))])
```

## State machine

```text
BLOCKED --all dependencies DONE--> READY --claim--> RUNNING
                                           | heartbeat extends lease
                                           v
                                      VERIFYING --pass--> DONE
                                           | fail
                                           v
                                         FAILED

RUNNING or VERIFYING --lease expiry--> READY
```
