from __future__ import annotations

import json
import os
import unittest
from unittest.mock import patch

from agent_demo import run_agent_demo
from agents import GeminiInteractionsClient, GroqResponsesClient
from agents.openai_agents import _gemini_schema
from shared.models import TaskStatus


class FakeModelClient:
    def __init__(self) -> None:
        self.calls = 0

    async def generate(self, instructions, prompt, *, schema=None):
        del prompt, schema
        self.calls += 1
        if "task-planning" in instructions:
            return json.dumps({"tasks": [
                {"task_id": "inspect_evidence", "dependencies": [], "instructions": "Inspect evidence"},
                {"task_id": "check_policy", "dependencies": [], "instructions": "Check policy"},
                {"task_id": "recommend_resolution", "dependencies": ["inspect_evidence", "check_policy"], "instructions": "Recommend"},
                {"task_id": "draft_response", "dependencies": ["recommend_resolution"], "instructions": "Draft response"},
            ]}), 20
        return json.dumps({"summary": "Completed bounded analysis", "evidence": ["case facts"],
                           "recommendation": "replace after authorized human review"}), 50


class FailingWorkerModelClient(FakeModelClient):
    async def generate(self, instructions, prompt, *, schema=None):
        if "task-planning" in instructions:
            return await super().generate(instructions, prompt, schema=schema)
        raise RuntimeError("provider quota exhausted")


class AgentIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_planner_and_workers_complete_with_checkpoint_recovery(self):
        client = FakeModelClient()
        result = await run_agent_demo(client, verbose=False)
        self.assertTrue(all(task.status is TaskStatus.DONE for task in result["plane"].list_tasks()))
        self.assertEqual(result["metrics"].worker_failures, 1)
        self.assertEqual(result["metrics"].recoveries, 1)
        self.assertEqual(client.calls, 5)  # planner + four tasks; recovery makes no duplicate call

    async def test_worker_provider_failure_surfaces_original_error(self):
        with self.assertRaisesRegex(RuntimeError, "provider quota exhausted"):
            await run_agent_demo(FailingWorkerModelClient(), verbose=False)

    async def test_gemini_client_requires_its_own_environment_key(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "GEMINI_API_KEY"):
                await GeminiInteractionsClient().generate("instructions", "prompt")

    async def test_groq_client_requires_its_own_environment_key(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "GROQ_API_KEY"):
                await GroqResponsesClient().generate("instructions", "prompt")

    def test_gemini_schema_removes_nonportable_strictness_keyword(self):
        self.assertEqual(
            _gemini_schema({"type": "object", "additionalProperties": False,
                            "properties": {"name": {"type": "string"}}}),
            {"type": "object", "properties": {"name": {"type": "string"}}},
        )


if __name__ == "__main__":
    unittest.main()
