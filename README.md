# Agent Orchestrator

A small, dependency-light Python engine for running multi-step agent (or
plain background-job) workflows safely: **idempotent execution**, **retries
with jittered exponential backoff**, and a **fully observable state
machine** with a queryable audit trail for every task.

This isn't a toy that only demos the happy path. It's built around the
failure modes that actually bite in production: a caller retrying a request
it isn't sure succeeded, a step that fails transiently and needs backoff
instead of an immediate hammering retry, and the need to answer "what is
this task doing, and how did it get there" without attaching a debugger or
grepping logs.

## Why this exists

Most "agent orchestrator" toy projects are a for-loop calling an LLM. The
interesting engineering problems are the same ones that show up in any
distributed task system — idempotency, retry safety, and observability —
just applied to agent steps instead of general background jobs. This
project is a deliberately small, readable implementation of those three
things, plus a thin FastAPI layer so the state is inspectable over HTTP
instead of only from inside the process that's running it.

It's the same shape of problem as a Kafka-worker-queue pattern I used in
production for bursty geospatial compute (don't pay for idle capacity, do
handle retries safely), and the same instinct behind an OTA fleet-update
path that never marks a device "upgraded" until the transfer is verified —
here, a task is never marked `SUCCEEDED` until the step function actually
returns cleanly, and a caller can retry the *request* as many times as it
wants without the *work* running twice.

## Design

- **Idempotency** — every submission carries an `idempotency_key`. The
  store enforces uniqueness on that key at the schema level (a `UNIQUE`
  SQLite constraint, not a check-then-act race in application code), so two
  callers submitting "the same" task at the same instant resolve safely
  instead of double-executing.
- **Retries** — failures are retried with exponential backoff and jitter
  (`RetryPolicy`) up to `max_attempts`, then the task moves to
  `DEAD_LETTER` with the last error recorded. Jitter matters at scale:
  without it, a batch of tasks that fail together retries in lockstep and
  re-hits the same downstream dependency at the same moment.
- **Observable state machine** — every task moves through an explicit
  `TaskState` (`PENDING → RUNNING → (RETRYING) → SUCCEEDED | DEAD_LETTER`),
  and every transition is appended to a durable history with a timestamp,
  attempt number, and a human-readable detail string. Nothing about a
  task's status is implicit.
- **Durable store, not an in-memory dict** — state lives in SQLite (trivially
  swappable for Postgres; the interface is the point) so a process restart
  doesn't lose the idempotency guarantee along with everything else.
- **Level-triggered, not edge-triggered** — re-running `run()` against a
  task that already converged (`SUCCEEDED` or `DEAD_LETTER`) is a safe
  no-op. This is the same reconciliation idea behind a Kubernetes
  controller: converge toward a terminal state, don't react to a one-shot
  event you might have already handled.

## What's deliberately out of scope

There's no distributed queue, no multi-worker coordination, and no real LLM
calls. Those are separate, well-understood problems (a message broker, a
lock/lease protocol, an LLM client) and bolting on a fake version of any of
them would make the repo bigger without making the interesting part —
idempotency, retry safety, and observability — any clearer. The `Step`
interface is intentionally "any callable that takes a payload dict," so
swapping in a real LLM call or a distributed queue behind `Orchestrator.run`
is a follow-up, not a redesign.

## Project layout

```
orchestrator/
  models.py   # TaskState, TaskRecord, StateTransition
  store.py    # SQLite-backed, idempotency-key-unique persistence
  engine.py   # Orchestrator: submit / run / retry / dead-letter logic
  api.py      # FastAPI read layer for inspecting task state over HTTP
examples/
  pipeline_demo.py   # research -> draft -> review -> publish, with a
                      # deliberately flaky review step to exercise retries
tests/
  test_engine.py      # idempotency, retries, backoff, dead-letter
  test_store.py        # persistence + schema-level uniqueness
```

## Running it

```bash
pip install -r requirements.txt

# Run the demo pipeline and watch retries/backoff play out:
python -m examples.pipeline_demo

# Run the test suite:
pytest -v

# Run the inspection API (then hit http://localhost:8000/tasks):
python -c "
import uvicorn
from orchestrator import Orchestrator
from orchestrator.api import create_app
uvicorn.run(create_app(Orchestrator()), host='0.0.0.0', port=8000)
"
```

## Example output

Running `python -m examples.pipeline_demo` prints the state history for the
deliberately-flaky review step, e.g.:

```
[review]   succeeded -> {'approved_draft': '...', 'review_attempts': 3}
  state history for the review task (this is the audit trail an operator or another agent can query):
           - -> pending    attempt=0  submitted
      pending -> running    attempt=1  attempt 1
      running -> retrying   attempt=1  attempt 1 failed: review service timed out...; retrying in 0.006s
     retrying -> running    attempt=2  attempt 2
      running -> retrying   attempt=2  attempt 2 failed: review service timed out...; retrying in 0.011s
     retrying -> running    attempt=3  attempt 3
      running -> succeeded  attempt=3  completed
re-submission returned the same task_id - no duplicate publish occurred.
```

## License

MIT — see [LICENSE](LICENSE).
