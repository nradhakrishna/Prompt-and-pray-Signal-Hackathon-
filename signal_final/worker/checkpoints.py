"""Crash-safe, atomic local checkpoint persistence."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class Checkpoint:
    checkpoint_id: str
    task_id: str
    attempt: int
    sequence: int
    state: Mapping[str, Any]


class FileCheckpointStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, task_id: str) -> Path:
        safe = hashlib.sha256(task_id.encode()).hexdigest()
        return self.root / f"{safe}.json"

    async def save(self, task_id: str, attempt: int, sequence: int, state: Mapping[str, Any]) -> Checkpoint:
        body = {"task_id": task_id, "attempt": attempt, "sequence": sequence, "state": dict(state)}
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
        checkpoint_id = hashlib.sha256(canonical.encode()).hexdigest()
        data = json.dumps({**body, "checkpoint_id": checkpoint_id}, sort_keys=True)
        write = asyncio.create_task(asyncio.to_thread(self._atomic_write, self._path(task_id), data))
        try:
            await asyncio.shield(write)
        except asyncio.CancelledError:
            # Python cannot cancel a filesystem thread. Drain it before allowing
            # process-level cancellation cleanup to continue.
            await write
            raise
        return Checkpoint(checkpoint_id, task_id, attempt, sequence, dict(state))

    @staticmethod
    def _atomic_write(path: Path, data: str) -> None:
        temp = path.with_suffix(f".{os.getpid()}.{uuid.uuid4().hex}.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)

    async def load(self, task_id: str) -> Checkpoint | None:
        path = self._path(task_id)
        if not path.exists():
            return None
        raw = await asyncio.to_thread(path.read_text, encoding="utf-8")
        value = json.loads(raw)
        return Checkpoint(value["checkpoint_id"], value["task_id"], value["attempt"], value["sequence"], value["state"])
