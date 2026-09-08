"""Core data model for the orchestrator.

Every task moves through an explicit, observable state machine rather than
being a bare function call. That is deliberate: the whole point of this
project is that you should always be able to answer "what is this task doing
right now, and how did it get there" without attaching a debugger.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TaskState(str, Enum):
    """Level-triggered states, not events.

    A task's state is always one of these values at any point in time - the
    engine reconciles toward SUCCEEDED (or DEAD_LETTER) rather than reacting
    to a one-shot event stream, which is what makes retries and restarts
    safe: re-running the reconciliation loop against a task that already
    finished is a no-op, not a bug.
    """

    PENDING = "pending"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


@dataclass
class StateTransition:
    """One entry in a task's audit trail."""

    from_state: Optional[TaskState]
    to_state: TaskState
    at: datetime = field(default_factory=utcnow)
    attempt: int = 0
    detail: str = ""


@dataclass
class TaskRecord:
    """Everything the system knows about one task.

    `idempotency_key` is what makes re-submission safe - if a caller submits
    the same logical task twice (a retried webhook, a re-run script, an
    agent that isn't sure whether its last call succeeded), the engine
    returns the existing record instead of doing the work twice.
    """

    task_id: str
    idempotency_key: str
    step_name: str
    payload: dict[str, Any]
    state: TaskState = TaskState.PENDING
    attempt: int = 0
    max_attempts: int = 3
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)
    history: list[StateTransition] = field(default_factory=list)

    def record_transition(self, to_state: TaskState, detail: str = "") -> None:
        # `state` defaults to PENDING before any transition has actually
        # been recorded, so the *first* transition has no real prior state -
        # treat it as None (rendered as "-") rather than a fake PENDING ->
        # PENDING self-loop.
        from_state = self.state if self.history else None
        self.history.append(
            StateTransition(
                from_state=from_state,
                to_state=to_state,
                attempt=self.attempt,
                detail=detail,
            )
        )
        self.state = to_state
        self.updated_at = utcnow()
