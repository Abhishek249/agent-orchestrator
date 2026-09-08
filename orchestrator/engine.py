"""The orchestration engine.

Design intent: an agent (or a human retrying a script) should be able to
call `submit()` for the same logical unit of work as many times as it wants
without side effects compounding, and every execution should leave behind an
audit trail that answers "what happened, and why" without needing logs from
the process that ran it.

This mirrors two things already proven out in production elsewhere: a
Kafka-worker-queue pattern used for bursty geospatial compute (the same
"don't pay for idle capacity, do handle retries safely" shape), and an
OTA fleet upgrade path that never activates anything it hasn't verified
first. Here, "activate" is "mark SUCCEEDED", and it only happens once a step
function returns cleanly.
"""
from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Optional

from .models import TaskRecord, TaskState
from .store import TaskStore

Step = Callable[[dict[str, Any]], Any]
"""A step is any callable that takes a payload dict and returns a JSON-
serializable result, or raises on failure. Swap in a real LLM call, a tool
invocation, or a plain function - the engine does not care."""


class DeadLetterError(Exception):
    """Raised (and also recorded on the task) once max_attempts is exhausted."""

    def __init__(self, task: TaskRecord):
        self.task = task
        super().__init__(
            f"task {task.task_id} ({task.step_name}) exhausted "
            f"{task.max_attempts} attempts: {task.error}"
        )


@dataclass
class RetryPolicy:
    """Exponential backoff with jitter, and an explicit ceiling.

    Jitter matters at scale: without it, every failed task in a batch backs
    off in lockstep and re-hits the same downstream dependency at the same
    moment, which is exactly how a transient blip turns into a thundering
    herd.
    """

    max_attempts: int = 3
    base_delay_s: float = 0.05
    max_delay_s: float = 2.0
    jitter: bool = True

    def delay_for(self, attempt: int) -> float:
        delay = min(self.max_delay_s, self.base_delay_s * (2 ** (attempt - 1)))
        if self.jitter:
            delay = random.uniform(0, delay)
        return delay


class Orchestrator:
    """Submits, executes, and tracks tasks against a registry of named steps."""

    def __init__(self, store: Optional[TaskStore] = None, sleep_fn: Callable[[float], None] = time.sleep):
        self.store = store or TaskStore()
        self._steps: dict[str, Step] = {}
        self._sleep = sleep_fn

    def register_step(self, name: str, fn: Step) -> None:
        self._steps[name] = fn

    def submit(
        self,
        step_name: str,
        payload: dict[str, Any],
        idempotency_key: str,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> TaskRecord:
        """Idempotent submission: the same idempotency_key always maps to the
        same TaskRecord, so a caller can retry the *request* freely without
        the *work* running twice."""
        existing = self.store.find_by_idempotency_key(idempotency_key)
        if existing is not None:
            return existing

        policy = retry_policy or RetryPolicy()
        record = TaskRecord(
            task_id=str(uuid.uuid4()),
            idempotency_key=idempotency_key,
            step_name=step_name,
            payload=payload,
            max_attempts=policy.max_attempts,
        )
        record.record_transition(TaskState.PENDING, detail="submitted")
        self.store.create(record)
        return record

    def run(self, task_id: str, retry_policy: Optional[RetryPolicy] = None) -> TaskRecord:
        """Execute a task to completion (success or dead-letter), retrying
        transient failures in-process. In a real deployment this is what a
        worker pulling off a queue calls; the queue itself is out of scope
        for this project on purpose - the interesting part is the state
        machine and idempotency guarantees, not the transport."""
        record = self.store.get(task_id)
        if record is None:
            raise KeyError(f"no such task: {task_id}")
        if record.state in (TaskState.SUCCEEDED, TaskState.DEAD_LETTER):
            return record  # already converged; re-running is a safe no-op

        policy = retry_policy or RetryPolicy(max_attempts=record.max_attempts)
        step = self._steps.get(record.step_name)
        if step is None:
            raise KeyError(f"no step registered for {record.step_name!r}")

        while True:
            record.attempt += 1
            record.record_transition(TaskState.RUNNING, detail=f"attempt {record.attempt}")
            self.store.save(record, record.history[-1])

            try:
                result = step(record.payload)
            except Exception as exc:  # noqa: BLE001 - step failures are data, not bugs
                record.error = str(exc)
                if record.attempt >= record.max_attempts:
                    record.record_transition(
                        TaskState.DEAD_LETTER,
                        detail=f"exhausted after {record.attempt} attempts: {exc}",
                    )
                    self.store.save(record, record.history[-1])
                    raise DeadLetterError(record) from exc

                delay = policy.delay_for(record.attempt)
                record.record_transition(
                    TaskState.RETRYING, detail=f"attempt {record.attempt} failed: {exc}; retrying in {delay:.3f}s"
                )
                self.store.save(record, record.history[-1])
                self._sleep(delay)
                continue

            record.result = result
            record.error = None
            record.record_transition(TaskState.SUCCEEDED, detail="completed")
            self.store.save(record, record.history[-1])
            return record

    def submit_and_run(
        self,
        step_name: str,
        payload: dict[str, Any],
        idempotency_key: str,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> TaskRecord:
        record = self.submit(step_name, payload, idempotency_key, retry_policy)
        if record.state in (TaskState.SUCCEEDED, TaskState.DEAD_LETTER):
            return record
        try:
            return self.run(record.task_id, retry_policy)
        except DeadLetterError as exc:
            return exc.task
