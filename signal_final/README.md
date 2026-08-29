# Distributed Checkout Workflow (Parts 1, 2, and 3)

This project demonstrates one complete distributed workflow:

```text
Auth ───┐
        ├──> Checkout ──> Integration
Cart ───┘
```

Auth and Cart are initially runnable. Checkout remains blocked until both pass
Oracle validation, and Integration remains blocked until Checkout passes. The
demo crashes the first Cart worker after it saves a checkpoint, expires its
lease, and resumes Cart on a replacement worker.

## Ownership boundaries

- `control/` (Person 1) owns DAG scheduling, task state, leases, dependency
  release, idempotent commits, and storage.
- `worker/` (Person 2) owns heartbeats, checkpoint/resume, execution, and
  construction of the shared `WorkerResult`.
- `oracle/` (Person 3) consumes that `WorkerResult`, runs declared deterministic
  validation/test commands, creates `OracleResult`, attributes faults, updates
  the ledger and metrics, and calls only the public P1 task-state API.
- `shared/` contains the canonical cross-subsystem models and protocol.

The Oracle accepts only a task in `VERIFYING` whose worker and attempt match the
staged P2 result. PASS calls `mark_done()`, which releases dependents. A worker or
integration fault calls `mark_failed()` and may slash the responsible worker. A
bad test is terminal but neutral to the worker. An infrastructure fault leaves
the task in `VERIFYING`, records no ledger settlement, and can be retried.

## Fault attribution and ledger

The four categories are:

- `WORKER_ERROR`: empty, incorrect, or incomplete worker output; slash.
- `TEST_ERROR`: invalid Oracle test definition; do not slash.
- `INTEGRATION_ERROR`: a valid deterministic test command rejected the output;
  slash in this simple demo policy.
- `INFRA_ERROR`: timeout, unavailable runner, stale delivery, or Oracle failure;
  keep retryable and do not slash.

Ledger settlement is idempotent per `task_id:attempt`. PASS rewards 5 points and
accountable failures slash 10 points; defaults are configurable in `Ledger`.
The ledger affects reputation only and never scheduler liveness.

## Deterministic versus agentic components

The entire shipped demo is deterministic. The scheduler, worker runtime,
checkpoint store, `StepExecutor`, Oracle, test runner, attribution, ledger, and
metrics do not call an LLM.

The production Worker Agent hook is `TaskExecutor.execute(...)` in
`worker/executors.py`; an LLM-backed implementation can replace `StepExecutor`
without changing leases, checkpoints, `WorkerResult`, or Oracle behavior.

## Model-backed agent demo

`agent_demo.py` now supplies that implementation. `AgentTaskPlanner` uses a
model to decompose a damaged-delivery support case into a DAG, while
`OpenAIWorkerExecutor` uses model-backed workers to solve the claimed tasks.
Both use structured outputs through the OpenAI Responses API. P1 still validates
the DAG and owns scheduling; P3 still accepts or rejects results using declared,
deterministic checks.

The demo crashes one worker after its expensive model output is checkpointed.
After lease expiry, a replacement worker loads that output and commits it
without making a duplicate model call.

```text
$env:OPENAI_API_KEY="your-key"
python agent_demo.py --model gpt-5.4-mini
```

Google AI Studio / Gemini:

```text
$env:GEMINI_API_KEY="your-key"
python agent_demo.py --provider gemini --model gemini-3.7-flash
```

Or use the safe Windows launcher. It prompts for a missing key without showing
it, saves it to the current user's environment, and enables timeout retries:

```text
powershell -ExecutionPolicy Bypass -File .\run_gemini.ps1
```

Groq (fast OpenAI-compatible Responses API):

```text
powershell -ExecutionPolicy Bypass -File .\run_groq.ps1
```

The default Groq model is `openai/gpt-oss-20b`. The launcher securely prompts
for `GROQ_API_KEY` when it is not already available.

Keys are read from the process environment and must never be committed to the
repository. PowerShell environment assignments apply only to that terminal
session unless saved through Windows environment settings.

The test suite uses a fake model client so agent orchestration and the exact
model-call count remain deterministic and do not spend API credits.

There is currently no Test-Gen Agent. Its exact future insertion point is before
`ControlPlane.submit()`: generate a reviewed, immutable `payload["validation"]`
specification (for example, expected outputs and a `test_command` argument list).
The Oracle must continue to execute that frozen specification deterministically;
it should not ask an LLM to judge a worker result at validation time.

## Run

Person 3 tests only:

```text
python -m unittest tests.test_oracle tests.test_checkout_demo -v
```

Full Parts 1–3 test suite:

```text
python -m unittest discover -s tests -v
```

Demo:

```text
python demo.py
```

The demo prints tasks completed, worker failures, recoveries, token savings,
dependency violations, duplicate commits, and final PASS.

## Dependencies

The in-memory demo, Oracle, metrics, ledger, and all tests use only Python 3.11+
standard-library modules. No new package was required for Person 3. `redis` in
`requirements.txt` remains optional and is needed only for `RedisTaskStore`:

```text
python -m pip install -r requirements.txt
```
