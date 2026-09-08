"""A thin FastAPI layer for inspecting orchestrator state.

This is deliberately a read/inspect API, not a way to bypass the engine's
in-process execution. The point of including it at all is observability:
an operator (or another agent) should be able to ask "what is task X doing"
without SSHing into a box, which is the same instinct behind everything
else in this repo.
"""
from __future__ import annotations

from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .engine import Orchestrator
from .models import TaskState


class TaskOut(BaseModel):
    task_id: str
    idempotency_key: str
    step_name: str
    state: TaskState
    attempt: int
    max_attempts: int
    result: Optional[Any] = None
    error: Optional[str] = None

    @classmethod
    def from_record(cls, r) -> "TaskOut":
        return cls(
            task_id=r.task_id,
            idempotency_key=r.idempotency_key,
            step_name=r.step_name,
            state=r.state,
            attempt=r.attempt,
            max_attempts=r.max_attempts,
            result=r.result,
            error=r.error,
        )


class TransitionOut(BaseModel):
    from_state: Optional[TaskState]
    to_state: TaskState
    at: str
    attempt: int
    detail: str


def create_app(orchestrator: Orchestrator) -> FastAPI:
    app = FastAPI(title="Agent Orchestrator", version="0.1.0")

    @app.get("/tasks", response_model=list[TaskOut])
    def list_tasks():
        return [TaskOut.from_record(r) for r in orchestrator.store.list_all()]

    @app.get("/tasks/{task_id}", response_model=TaskOut)
    def get_task(task_id: str):
        record = orchestrator.store.get(task_id)
        if record is None:
            raise HTTPException(status_code=404, detail="task not found")
        return TaskOut.from_record(record)

    @app.get("/tasks/{task_id}/history", response_model=list[TransitionOut])
    def get_history(task_id: str):
        record = orchestrator.store.get(task_id)
        if record is None:
            raise HTTPException(status_code=404, detail="task not found")
        return [
            TransitionOut(
                from_state=t.from_state,
                to_state=t.to_state,
                at=t.at.isoformat(),
                attempt=t.attempt,
                detail=t.detail,
            )
            for t in record.history
        ]

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    return app
