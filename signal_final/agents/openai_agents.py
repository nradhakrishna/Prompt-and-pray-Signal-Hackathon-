"""Provider-backed task-planning and worker agents."""

from __future__ import annotations

import asyncio
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from shared.models import Task
from worker.executors import CheckpointCallback, ExecutionOutput


class ModelClient(Protocol):
    async def generate(self, instructions: str, prompt: str, *, schema: Mapping[str, Any] | None = None) -> tuple[str, int]: ...


@dataclass(slots=True)
class OpenAIResponsesClient:
    """Dependency-free client for the OpenAI Responses API."""

    model: str = "gpt-5.4-mini"
    api_key: str | None = None
    base_url: str = "https://api.openai.com/v1/responses"
    timeout: float = 90.0

    def __post_init__(self) -> None:
        self.api_key = self.api_key or os.getenv("OPENAI_API_KEY")

    async def generate(self, instructions: str, prompt: str, *, schema: Mapping[str, Any] | None = None) -> tuple[str, int]:
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY is required for the live agent demo")
        return await asyncio.to_thread(self._generate_sync, instructions, prompt, schema)

    def _generate_sync(self, instructions: str, prompt: str, schema: Mapping[str, Any] | None) -> tuple[str, int]:
        body: dict[str, Any] = {"model": self.model, "instructions": instructions, "input": prompt, "store": False}
        if schema is not None:
            body["text"] = {"format": {"type": "json_schema", "name": "agent_output", "strict": True, "schema": dict(schema)}}
        request = urllib.request.Request(
            self.base_url, data=json.dumps(body).encode(),
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}, method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                value = json.loads(response.read())
        except urllib.error.HTTPError as exc:
            details = exc.read().decode(errors="replace")
            raise RuntimeError(f"OpenAI Responses API returned HTTP {exc.code}: {details[:500]}") from exc
        text = value.get("output_text")
        if not text:
            text = "".join(
                content.get("text", "") for item in value.get("output", [])
                for content in item.get("content", []) if content.get("type") == "output_text"
            )
        if not text:
            raise RuntimeError("model response contained no output text")
        return str(text), int((value.get("usage") or {}).get("total_tokens", 0))


@dataclass(slots=True)
class GroqResponsesClient:
    """Groq's OpenAI-compatible Responses API with bounded retries."""

    model: str = "openai/gpt-oss-20b"
    api_key: str | None = None
    base_url: str = "https://api.groq.com/openai/v1/responses"
    timeout: float = 120.0
    retries: int = 2

    def __post_init__(self) -> None:
        self.api_key = self.api_key or os.getenv("GROQ_API_KEY")

    async def generate(self, instructions: str, prompt: str, *, schema: Mapping[str, Any] | None = None) -> tuple[str, int]:
        if not self.api_key:
            raise RuntimeError("GROQ_API_KEY is required for the live Groq agent demo")
        return await asyncio.to_thread(self._generate_sync, instructions, prompt, schema)

    def _generate_sync(self, instructions: str, prompt: str, schema: Mapping[str, Any] | None) -> tuple[str, int]:
        # Groq's Responses API currently rejects OpenAI's `store` field.
        body: dict[str, Any] = {
            "model": self.model,
            "instructions": instructions,
            "input": prompt,
        }
        if schema is not None:
            body["text"] = {"format": {
                "type": "json_schema", "name": "agent_output", "strict": True,
                "schema": dict(schema),
            }}
        request = urllib.request.Request(
            self.base_url, data=json.dumps(body).encode(),
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}, method="POST",
        )
        value: dict[str, Any] | None = None
        for attempt in range(self.retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    value = json.loads(response.read())
                break
            except urllib.error.HTTPError as exc:
                details = exc.read().decode(errors="replace")
                if exc.code not in {429, 500, 502, 503, 504} or attempt >= self.retries:
                    raise RuntimeError(
                        f"Groq Responses API returned HTTP {exc.code}: {details[:500]}"
                    ) from exc
            except (TimeoutError, urllib.error.URLError) as exc:
                if attempt >= self.retries:
                    raise RuntimeError(
                        f"Groq request failed after {self.retries + 1} attempts: {exc}"
                    ) from exc
            time.sleep(2 ** attempt)
        if value is None:
            raise RuntimeError("Groq request ended without a response")
        text = value.get("output_text")
        if not text:
            text = "".join(
                content.get("text", "") for item in value.get("output", [])
                for content in item.get("content", []) if content.get("type") == "output_text"
            )
        if not text:
            raise RuntimeError("Groq response contained no output text")
        return str(text), int((value.get("usage") or {}).get("total_tokens", 0))


def _gemini_schema(value: Any) -> Any:
    """Keep the portable JSON Schema subset accepted by Gemini."""
    if isinstance(value, dict):
        return {key: _gemini_schema(item) for key, item in value.items()
                if key not in {"additionalProperties"}}
    if isinstance(value, list):
        return [_gemini_schema(item) for item in value]
    return value


@dataclass(slots=True)
class GeminiInteractionsClient:
    """Dependency-free Google Gemini Interactions API client."""

    model: str = "gemini-3.7-flash"
    api_key: str | None = None
    base_url: str = "https://generativelanguage.googleapis.com/v1beta/interactions"
    timeout: float = 300.0
    retries: int = 2

    def __post_init__(self) -> None:
        self.api_key = self.api_key or os.getenv("GEMINI_API_KEY")

    async def generate(self, instructions: str, prompt: str, *, schema: Mapping[str, Any] | None = None) -> tuple[str, int]:
        if not self.api_key:
            raise RuntimeError("GEMINI_API_KEY is required for the live Gemini agent demo")
        return await asyncio.to_thread(self._generate_sync, instructions, prompt, schema)

    def _generate_sync(self, instructions: str, prompt: str, schema: Mapping[str, Any] | None) -> tuple[str, int]:
        body: dict[str, Any] = {
            "model": self.model,
            "input": f"Instructions:\n{instructions}\n\nTask:\n{prompt}",
            "store": False,
        }
        if schema is not None:
            body["response_format"] = {
                "type": "text", "mime_type": "application/json",
                "schema": _gemini_schema(dict(schema)),
            }
        request = urllib.request.Request(
            self.base_url, data=json.dumps(body).encode(),
            headers={"x-goog-api-key": self.api_key, "Content-Type": "application/json"}, method="POST",
        )
        value: dict[str, Any] | None = None
        for attempt in range(self.retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    value = json.loads(response.read())
                break
            except urllib.error.HTTPError as exc:
                details = exc.read().decode(errors="replace")
                if exc.code not in {429, 500, 502, 503, 504} or attempt >= self.retries:
                    raise RuntimeError(
                        f"Gemini Interactions API returned HTTP {exc.code}: {details[:500]}"
                    ) from exc
            except (TimeoutError, urllib.error.URLError) as exc:
                if attempt >= self.retries:
                    raise RuntimeError(
                        f"Gemini request failed after {self.retries + 1} attempts: {exc}"
                    ) from exc
            time.sleep(2 ** attempt)
        if value is None:
            raise RuntimeError("Gemini request ended without a response")
        text = value.get("output_text")
        if not text:
            text = "".join(
                content.get("text", "") for step in value.get("steps", [])
                if step.get("type") == "model_output" for content in step.get("content", [])
                if content.get("type") == "text"
            )
        if not text:
            raise RuntimeError("Gemini response contained no output text")
        return str(text), int((value.get("usage") or {}).get("total_tokens", 0))


PLAN_SCHEMA = {
    "type": "object", "properties": {"tasks": {"type": "array", "minItems": 2, "maxItems": 8,
        "items": {"type": "object", "properties": {
            "task_id": {"type": "string"}, "dependencies": {"type": "array", "items": {"type": "string"}},
            "instructions": {"type": "string"}},
            "required": ["task_id", "dependencies", "instructions"], "additionalProperties": False}}},
    "required": ["tasks"], "additionalProperties": False,
}


class AgentTaskPlanner:
    """Uses a model to turn a real-world objective into a bounded DAG."""

    def __init__(self, client: ModelClient) -> None:
        self.client = client

    async def plan(self, problem: str) -> tuple[list[Task], int]:
        text, tokens = await self.client.generate(
            "You are a task-planning agent. Decompose the objective into a small acyclic DAG. Use safe snake_case task IDs. "
            "Each worker must return a JSON object with summary, evidence, and recommendation. Do not schedule tasks.",
            problem, schema=PLAN_SCHEMA,
        )
        rows = json.loads(text)["tasks"]
        ids = [str(row["task_id"]) for row in rows]
        if len(ids) != len(set(ids)) or any(not task_id.replace("_", "").isalnum() for task_id in ids):
            raise ValueError("planner returned invalid or duplicate task IDs")
        known = set(ids)
        tasks = []
        for row in rows:
            dependencies = tuple(str(item) for item in row["dependencies"])
            if not set(dependencies) <= known:
                raise ValueError("planner returned an unknown dependency")
            tasks.append(Task(str(row["task_id"]), dependencies=dependencies, payload={
                "agent_instructions": str(row["instructions"]), "problem": problem,
                "validation": {"required_substrings": ['"summary"', '"evidence"', '"recommendation"']},
            }))
        return tasks, tokens


WORK_SCHEMA = {
    "type": "object", "properties": {"summary": {"type": "string"},
        "evidence": {"type": "array", "items": {"type": "string"}}, "recommendation": {"type": "string"}},
    "required": ["summary", "evidence", "recommendation"], "additionalProperties": False,
}


class OpenAIWorkerExecutor:
    """Model-backed P2 executor with checkpoint-safe generation reuse."""

    def __init__(self, client: ModelClient) -> None:
        self.client = client

    async def execute(self, task: Task, state: Mapping[str, Any] | None, checkpoint: CheckpointCallback) -> ExecutionOutput:
        state = dict(state or {})
        if "generated_output" in state:
            return ExecutionOutput(str(state["generated_output"]), int(state.get("token_cost", 0)))
        await checkpoint(1, {"stage": "prompt_prepared"})
        prompt = (f"Overall problem:\n{task.payload.get('problem', '')}\n\nAssigned task:\n"
                  f"{task.payload.get('agent_instructions', '')}\n\nDo not claim external actions.")
        output, tokens = await self.client.generate(
            "You are an autonomous bounded worker agent. Solve only your assigned task and return the required structured result.",
            prompt, schema=WORK_SCHEMA,
        )
        await checkpoint(2, {"stage": "model_completed", "generated_output": output, "token_cost": tokens})
        return ExecutionOutput(output, tokens)
