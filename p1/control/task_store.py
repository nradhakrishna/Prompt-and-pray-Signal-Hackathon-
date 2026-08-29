"""Durable task stores used by the lease-based scheduler.

The in-memory implementation exists for local development and deterministic
tests.  ``RedisTaskStore`` keeps the same behaviour and performs state changes
inside Redis Lua scripts so competing schedulers cannot double-claim work.
"""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from typing import Any, Iterable, Mapping, Protocol

from shared.models import Task, TaskStatus, WorkerResult

from .dag import TaskDAG


class TaskStoreError(RuntimeError):
    """Base error raised by a task-store operation."""


class TaskNotFoundError(TaskStoreError):
    """Raised for an unknown task id."""


class DuplicateTaskError(TaskStoreError):
    """Raised when initialising an already-known task id."""


class InvalidTaskTransition(TaskStoreError):
    """Raised when an operation would violate the task state machine."""


class TaskStore(Protocol):
    """Storage boundary; the scheduler does not know Redis implementation details."""

    def add_tasks(self, tasks: Iterable[Task]) -> None: ...

    def claim_next(self, worker_id: str, now: float) -> Task | None: ...

    def heartbeat(self, task_id: str, worker_id: str, now: float) -> Task: ...

    def mark_verifying(self, task_id: str, now: float) -> Task: ...

    def mark_done(
        self, task_id: str, result: Mapping[str, Any] | WorkerResult, now: float
    ) -> Task: ...

    def mark_failed(self, task_id: str, reason: str, now: float) -> Task: ...

    def requeue_expired(self, now: float) -> list[str]: ...

    def get_task(self, task_id: str) -> Task: ...

    def list_tasks(self) -> list[Task]: ...


def _normalise_result(
    task_id: str, result: Mapping[str, Any] | WorkerResult
) -> dict[str, Any]:
    data = result.to_dict() if isinstance(result, WorkerResult) else dict(result)
    if "task_id" in data and data["task_id"] != task_id:
        raise InvalidTaskTransition(
            f"result belongs to task {data['task_id']!r}, not {task_id!r}"
        )
    return data


def _assert_result_claim(task: Task, result: Mapping[str, Any]) -> None:
    """Reject a stale or foreign result when Worker Runtime supplies claim data."""

    if "attempt" in result and int(result["attempt"]) != task.attempt:
        raise InvalidTaskTransition(
            f"result attempt {result['attempt']} is stale; current attempt is {task.attempt}"
        )
    if "worker_id" in result and result["worker_id"] != task.worker_id:
        raise InvalidTaskTransition(
            f"result worker {result['worker_id']!r} does not own task {task.task_id!r}"
        )


class InMemoryTaskStore:
    """Thread-safe reference implementation used by the example and tests."""

    def __init__(self, lease_seconds: float = 30.0, verification_lease_seconds: float = 30.0):
        if lease_seconds <= 0 or verification_lease_seconds <= 0:
            raise ValueError("lease durations must be positive")
        self.lease_seconds = lease_seconds
        self.verification_lease_seconds = verification_lease_seconds
        self._tasks: dict[str, Task] = {}
        self._dependents: dict[str, set[str]] = {}
        self._ready: deque[str] = deque()
        self._resource_owners: dict[str, str] = {}
        self._lock = threading.RLock()

    def add_tasks(self, tasks: Iterable[Task]) -> None:
        submitted = [task.snapshot() for task in tasks]
        TaskDAG(submitted)  # validates the complete submission before mutation
        with self._lock:
            collisions = sorted(task.task_id for task in submitted if task.task_id in self._tasks)
            if collisions:
                raise DuplicateTaskError("tasks already exist: " + ", ".join(collisions))
            for task in submitted:
                # Runtime state belongs to the control plane, not submission input.
                task.status = TaskStatus.READY if not task.dependencies else TaskStatus.BLOCKED
                task.worker_id = None
                task.lease_expiry = None
                task.attempt = 0
                task.result = None
                task.failure_reason = None
                self._tasks[task.task_id] = task
                for dependency in task.dependencies:
                    self._dependents.setdefault(dependency, set()).add(task.task_id)
                if task.status is TaskStatus.READY:
                    self._ready.append(task.task_id)

    def claim_next(self, worker_id: str, now: float) -> Task | None:
        if not worker_id:
            raise ValueError("worker_id must be non-empty")
        with self._lock:
            # Scan at most the current queue length. Conflicting ready work stays
            # queued while another independent task may still make progress.
            for _ in range(len(self._ready)):
                task_id = self._ready.popleft()
                task = self._tasks[task_id]
                if task.status is not TaskStatus.READY:
                    continue  # stale delivery after an idempotent transition
                if not self._dependencies_done(task):
                    task.status = TaskStatus.BLOCKED
                    continue
                if not self._resources_available(task):
                    self._ready.append(task_id)
                    continue
                task.status = TaskStatus.RUNNING
                task.worker_id = worker_id
                task.lease_expiry = now + self.lease_seconds
                task.attempt += 1
                self._acquire_resources(task)
                return task.snapshot()
            return None

    def heartbeat(self, task_id: str, worker_id: str, now: float) -> Task:
        with self._lock:
            task = self._require(task_id)
            if task.status is not TaskStatus.RUNNING or task.worker_id != worker_id:
                raise InvalidTaskTransition(
                    f"only the current RUNNING worker may heartbeat task {task_id!r}"
                )
            if task.lease_expiry is None or task.lease_expiry <= now:
                raise InvalidTaskTransition(f"lease for task {task_id!r} has expired")
            task.lease_expiry = now + self.lease_seconds
            return task.snapshot()

    def mark_verifying(self, task_id: str, now: float) -> Task:
        with self._lock:
            task = self._require(task_id)
            if task.status is TaskStatus.VERIFYING:
                return task.snapshot()
            if task.status is not TaskStatus.RUNNING:
                raise InvalidTaskTransition(
                    f"task {task_id!r} must be RUNNING before verification"
                )
            task.status = TaskStatus.VERIFYING
            task.lease_expiry = now + self.verification_lease_seconds
            return task.snapshot()

    def mark_done(
        self, task_id: str, result: Mapping[str, Any] | WorkerResult, now: float
    ) -> Task:
        del now  # Store protocol is symmetric with Redis; completion does not need time here.
        result_data = _normalise_result(task_id, result)
        with self._lock:
            task = self._require(task_id)
            if task.status is TaskStatus.DONE:
                return task.snapshot()  # idempotent commit: never overwrite the first result
            if task.status not in (TaskStatus.RUNNING, TaskStatus.VERIFYING):
                raise InvalidTaskTransition(f"task {task_id!r} is not completable")
            _assert_result_claim(task, result_data)
            task.status = TaskStatus.DONE
            task.result = result_data
            task.failure_reason = None
            task.lease_expiry = None
            self._release_resources(task)
            self._release_dependents(task_id)
            return task.snapshot()

    def mark_failed(self, task_id: str, reason: str, now: float) -> Task:
        del now
        if not reason:
            raise ValueError("failure reason must be non-empty")
        with self._lock:
            task = self._require(task_id)
            if task.status is TaskStatus.FAILED:
                return task.snapshot()
            if task.status not in (TaskStatus.RUNNING, TaskStatus.VERIFYING):
                raise InvalidTaskTransition(f"task {task_id!r} is not fail-able")
            task.status = TaskStatus.FAILED
            task.failure_reason = reason
            task.lease_expiry = None
            self._release_resources(task)
            return task.snapshot()

    def requeue_expired(self, now: float) -> list[str]:
        with self._lock:
            requeued: list[str] = []
            for task in self._tasks.values():
                if (
                    task.status in (TaskStatus.RUNNING, TaskStatus.VERIFYING)
                    and task.lease_expiry is not None
                    and task.lease_expiry <= now
                ):
                    self._release_resources(task)
                    task.status = TaskStatus.READY
                    task.worker_id = None
                    task.lease_expiry = None
                    self._ready.append(task.task_id)
                    requeued.append(task.task_id)
            return requeued

    def get_task(self, task_id: str) -> Task:
        with self._lock:
            return self._require(task_id).snapshot()

    def list_tasks(self) -> list[Task]:
        with self._lock:
            return [self._tasks[task_id].snapshot() for task_id in sorted(self._tasks)]

    def _require(self, task_id: str) -> Task:
        try:
            return self._tasks[task_id]
        except KeyError as exc:
            raise TaskNotFoundError(f"unknown task {task_id!r}") from exc

    def _dependencies_done(self, task: Task) -> bool:
        return all(self._tasks[dependency].status is TaskStatus.DONE for dependency in task.dependencies)

    def _resources_available(self, task: Task) -> bool:
        return all(
            resource not in self._resource_owners
            or self._resource_owners[resource] == task.task_id
            for resource in task.resources
        )

    def _acquire_resources(self, task: Task) -> None:
        for resource in task.resources:
            self._resource_owners[resource] = task.task_id

    def _release_resources(self, task: Task) -> None:
        for resource in task.resources:
            if self._resource_owners.get(resource) == task.task_id:
                del self._resource_owners[resource]

    def _release_dependents(self, completed_task_id: str) -> None:
        for child_id in sorted(self._dependents.get(completed_task_id, ())):
            child = self._tasks[child_id]
            if child.status is TaskStatus.BLOCKED and self._dependencies_done(child):
                child.status = TaskStatus.READY
                self._ready.append(child_id)


class RedisTaskStore:
    """Redis-backed durable store and ready queue.

    Redis is deliberately imported only when this class is used, so a fresh
    checkout can run the local demo and tests before the optional production
    dependency is installed.
    """

    _CLAIM_SCRIPT = r"""
local queue = KEYS[1]
local task_prefix = ARGV[1]
local resource_prefix = ARGV[2]
local worker_id = ARGV[3]
local now = tonumber(ARGV[4])
local lease_seconds = tonumber(ARGV[5])
local count = redis.call('LLEN', queue)
for index = 1, count do
  local task_id = redis.call('LPOP', queue)
  local raw = redis.call('GET', task_prefix .. task_id)
  if raw then
    local task = cjson.decode(raw)
    if task.status == 'READY' then
      local available = true
      for _, resource in ipairs(task.resources) do
        local owner = redis.call('GET', resource_prefix .. resource)
        if owner and owner ~= task_id then available = false break end
      end
      if available then
        task.status = 'RUNNING'
        task.worker_id = worker_id
        task.lease_expiry = now + lease_seconds
        task.attempt = task.attempt + 1
        for _, resource in ipairs(task.resources) do
          redis.call('SET', resource_prefix .. resource, task_id, 'PX', math.ceil(lease_seconds * 1000))
        end
        local encoded = cjson.encode(task)
        redis.call('SET', task_prefix .. task_id, encoded)
        return encoded
      end
      redis.call('RPUSH', queue, task_id)
    end
  end
end
return false
"""

    _HEARTBEAT_SCRIPT = r"""
local raw = redis.call('GET', KEYS[1])
if not raw then return redis.error_reply('TASK_NOT_FOUND') end
local task = cjson.decode(raw)
local now = tonumber(ARGV[1])
local lease_seconds = tonumber(ARGV[2])
local worker_id = ARGV[3]
local resource_prefix = ARGV[4]
if task.status ~= 'RUNNING' or task.worker_id ~= worker_id then return redis.error_reply('INVALID_TRANSITION') end
if not task.lease_expiry or task.lease_expiry <= now then return redis.error_reply('LEASE_EXPIRED') end
task.lease_expiry = now + lease_seconds
for _, resource in ipairs(task.resources) do
  local key = resource_prefix .. resource
  if redis.call('GET', key) == task.task_id then
    redis.call('PEXPIRE', key, math.ceil(lease_seconds * 1000))
  end
end
local encoded = cjson.encode(task)
redis.call('SET', KEYS[1], encoded)
return encoded
"""

    _VERIFYING_SCRIPT = r"""
local raw = redis.call('GET', KEYS[1])
if not raw then return redis.error_reply('TASK_NOT_FOUND') end
local task = cjson.decode(raw)
if task.status == 'VERIFYING' then return raw end
if task.status ~= 'RUNNING' then return redis.error_reply('INVALID_TRANSITION') end
task.status = 'VERIFYING'
task.lease_expiry = tonumber(ARGV[1]) + tonumber(ARGV[2])
for _, resource in ipairs(task.resources) do
  local key = ARGV[3] .. resource
  if redis.call('GET', key) == task.task_id then
    redis.call('PEXPIRE', key, math.ceil(tonumber(ARGV[2]) * 1000))
  end
end
local encoded = cjson.encode(task)
redis.call('SET', KEYS[1], encoded)
return encoded
"""

    _DONE_SCRIPT = r"""
local queue = KEYS[1]
local raw = redis.call('GET', KEYS[2])
if not raw then return redis.error_reply('TASK_NOT_FOUND') end
local task = cjson.decode(raw)
if task.status == 'DONE' then return raw end
if task.status ~= 'RUNNING' and task.status ~= 'VERIFYING' then return redis.error_reply('INVALID_TRANSITION') end
local result = cjson.decode(ARGV[1])
if result.attempt and tonumber(result.attempt) ~= tonumber(task.attempt) then return redis.error_reply('STALE_ATTEMPT') end
if result.worker_id and result.worker_id ~= task.worker_id then return redis.error_reply('FOREIGN_WORKER') end
task.status = 'DONE'
task.result = result
task.failure_reason = cjson.null
task.lease_expiry = cjson.null
for _, resource in ipairs(task.resources) do
  local key = ARGV[2] .. resource
  if redis.call('GET', key) == task.task_id then redis.call('DEL', key) end
end
redis.call('SET', KEYS[2], cjson.encode(task))
for _, child_id in ipairs(redis.call('SMEMBERS', ARGV[3] .. task.task_id)) do
  local child_key = ARGV[4] .. child_id
  local child_raw = redis.call('GET', child_key)
  if child_raw then
    local child = cjson.decode(child_raw)
    if child.status == 'BLOCKED' then
      local ready = true
      for _, dependency in ipairs(child.dependencies) do
        local dependency_raw = redis.call('GET', ARGV[4] .. dependency)
        if not dependency_raw or cjson.decode(dependency_raw).status ~= 'DONE' then ready = false break end
      end
      if ready then
        child.status = 'READY'
        redis.call('SET', child_key, cjson.encode(child))
        redis.call('RPUSH', queue, child_id)
      end
    end
  end
end
return redis.call('GET', KEYS[2])
"""

    _FAILED_SCRIPT = r"""
local raw = redis.call('GET', KEYS[1])
if not raw then return redis.error_reply('TASK_NOT_FOUND') end
local task = cjson.decode(raw)
if task.status == 'FAILED' then return raw end
if task.status ~= 'RUNNING' and task.status ~= 'VERIFYING' then return redis.error_reply('INVALID_TRANSITION') end
task.status = 'FAILED'
task.failure_reason = ARGV[1]
task.lease_expiry = cjson.null
for _, resource in ipairs(task.resources) do
  local key = ARGV[2] .. resource
  if redis.call('GET', key) == task.task_id then redis.call('DEL', key) end
end
local encoded = cjson.encode(task)
redis.call('SET', KEYS[1], encoded)
return encoded
"""

    _REQUEUE_SCRIPT = r"""
local task_ids = redis.call('SMEMBERS', KEYS[1])
local task_prefix = ARGV[1]
local resource_prefix = ARGV[2]
local now = tonumber(ARGV[3])
local requeued = {}
for _, task_id in ipairs(task_ids) do
  local key = task_prefix .. task_id
  local raw = redis.call('GET', key)
  if raw then
    local task = cjson.decode(raw)
    if (task.status == 'RUNNING' or task.status == 'VERIFYING') and task.lease_expiry and task.lease_expiry <= now then
      for _, resource in ipairs(task.resources) do
        local resource_key = resource_prefix .. resource
        if redis.call('GET', resource_key) == task_id then redis.call('DEL', resource_key) end
      end
      task.status = 'READY'
      task.worker_id = cjson.null
      task.lease_expiry = cjson.null
      redis.call('SET', key, cjson.encode(task))
      redis.call('RPUSH', KEYS[2], task_id)
      table.insert(requeued, task_id)
    end
  end
end
return requeued
"""

    def __init__(
        self,
        redis_client: Any | None = None,
        *,
        redis_url: str = "redis://localhost:6379/0",
        namespace: str = "control_plane",
        lease_seconds: float = 30.0,
        verification_lease_seconds: float = 30.0,
    ) -> None:
        if lease_seconds <= 0 or verification_lease_seconds <= 0:
            raise ValueError("lease durations must be positive")
        if not namespace or ":" in namespace:
            raise ValueError("namespace must be non-empty and cannot contain ':'")
        if redis_client is None:
            try:
                import redis  # type: ignore[import-not-found]
            except ImportError as exc:
                raise RuntimeError(
                    "RedisTaskStore requires the optional dependency. Run: pip install -r requirements.txt"
                ) from exc
            redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
        self._redis = redis_client
        self.namespace = namespace
        self.lease_seconds = lease_seconds
        self.verification_lease_seconds = verification_lease_seconds

    def add_tasks(self, tasks: Iterable[Task]) -> None:
        submitted = [task.snapshot() for task in tasks]
        TaskDAG(submitted)
        task_ids = [task.task_id for task in submitted]
        if not task_ids:
            return
        keys = [self._task_key(task_id) for task_id in task_ids]
        if self._redis.exists(*keys):
            collisions = [task_id for task_id in task_ids if self._redis.exists(self._task_key(task_id))]
            raise DuplicateTaskError("tasks already exist: " + ", ".join(sorted(collisions)))

        pipeline = self._redis.pipeline(transaction=True)
        for task in submitted:
            task.status = TaskStatus.READY if not task.dependencies else TaskStatus.BLOCKED
            task.worker_id = None
            task.lease_expiry = None
            task.attempt = 0
            task.result = None
            task.failure_reason = None
            pipeline.set(self._task_key(task.task_id), json.dumps(task.to_dict(), separators=(",", ":")))
            pipeline.sadd(self._task_ids_key, task.task_id)
            for dependency in task.dependencies:
                pipeline.sadd(self._dependents_key(dependency), task.task_id)
            if task.status is TaskStatus.READY:
                pipeline.rpush(self._ready_key, task.task_id)
        pipeline.execute()

    def claim_next(self, worker_id: str, now: float) -> Task | None:
        if not worker_id:
            raise ValueError("worker_id must be non-empty")
        response = self._eval(
            self._CLAIM_SCRIPT,
            [self._ready_key],
            [self._task_prefix, self._resource_prefix, worker_id, now, self.lease_seconds],
        )
        return self._task_or_none(response)

    def heartbeat(self, task_id: str, worker_id: str, now: float) -> Task:
        response = self._eval(
            self._HEARTBEAT_SCRIPT,
            [self._task_key(task_id)],
            [now, self.lease_seconds, worker_id, self._resource_prefix],
        )
        return self._task_required(response, task_id)

    def mark_verifying(self, task_id: str, now: float) -> Task:
        response = self._eval(
            self._VERIFYING_SCRIPT,
            [self._task_key(task_id)],
            [now, self.verification_lease_seconds, self._resource_prefix],
        )
        return self._task_required(response, task_id)

    def mark_done(
        self, task_id: str, result: Mapping[str, Any] | WorkerResult, now: float
    ) -> Task:
        del now
        result_data = _normalise_result(task_id, result)
        response = self._eval(
            self._DONE_SCRIPT,
            [self._ready_key, self._task_key(task_id)],
            [json.dumps(result_data, separators=(",", ":")), self._resource_prefix, self._dependents_prefix, self._task_prefix],
        )
        return self._task_required(response, task_id)

    def mark_failed(self, task_id: str, reason: str, now: float) -> Task:
        del now
        if not reason:
            raise ValueError("failure reason must be non-empty")
        response = self._eval(
            self._FAILED_SCRIPT,
            [self._task_key(task_id)],
            [reason, self._resource_prefix],
        )
        return self._task_required(response, task_id)

    def requeue_expired(self, now: float) -> list[str]:
        response = self._eval(
            self._REQUEUE_SCRIPT,
            [self._task_ids_key, self._ready_key],
            [self._task_prefix, self._resource_prefix, now],
        )
        return [self._text(task_id) for task_id in response]

    def get_task(self, task_id: str) -> Task:
        raw = self._redis.get(self._task_key(task_id))
        if raw is None:
            raise TaskNotFoundError(f"unknown task {task_id!r}")
        return Task.from_dict(json.loads(self._text(raw)))

    def list_tasks(self) -> list[Task]:
        task_ids = sorted(self._text(task_id) for task_id in self._redis.smembers(self._task_ids_key))
        return [self.get_task(task_id) for task_id in task_ids]

    @property
    def _task_prefix(self) -> str:
        return f"{self.namespace}:task:"

    @property
    def _resource_prefix(self) -> str:
        return f"{self.namespace}:resource:"

    @property
    def _dependents_prefix(self) -> str:
        return f"{self.namespace}:dependents:"

    @property
    def _task_ids_key(self) -> str:
        return f"{self.namespace}:task_ids"

    @property
    def _ready_key(self) -> str:
        return f"{self.namespace}:ready_queue"

    def _task_key(self, task_id: str) -> str:
        return f"{self._task_prefix}{task_id}"

    def _dependents_key(self, task_id: str) -> str:
        return f"{self._dependents_prefix}{task_id}"

    def _eval(self, script: str, keys: list[str], arguments: list[Any]) -> Any:
        try:
            return self._redis.eval(script, len(keys), *keys, *(str(argument) for argument in arguments))
        except Exception as exc:  # redis exposes ResponseError without a stable import here
            message = str(exc)
            if "TASK_NOT_FOUND" in message:
                raise TaskNotFoundError(message) from exc
            if any(code in message for code in ("INVALID_TRANSITION", "LEASE_EXPIRED", "STALE_ATTEMPT", "FOREIGN_WORKER")):
                raise InvalidTaskTransition(message) from exc
            raise

    @staticmethod
    def _text(value: Any) -> str:
        return value.decode("utf-8") if isinstance(value, bytes) else str(value)

    def _task_or_none(self, raw: Any) -> Task | None:
        return None if raw is None or raw is False else Task.from_dict(json.loads(self._text(raw)))

    def _task_required(self, raw: Any, task_id: str) -> Task:
        task = self._task_or_none(raw)
        if task is None:
            raise TaskNotFoundError(f"unknown task {task_id!r}")
        return task
