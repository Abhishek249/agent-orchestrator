"""Durable, inspectable storage for task records.

Two design decisions worth calling out:

1. SQLite, not a Python dict. An in-memory dict would make the demo simpler,
   but it would also hide the exact failure mode this project exists to
   solve: if the process restarts mid-task, an in-memory store loses the
   idempotency key along with everything else, and a retried caller looks
   like a brand-new request. Backing the store with SQLite (trivially
   swappable for Postgres later - the interface is the point) means the
   idempotency check survives a crash.

2. The idempotency key is UNIQUE at the schema level, not just checked in
   application code. A race between two callers submitting the same task at
   the same instant is resolved by the database, not by a check-then-act
   race in Python.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime
from typing import Optional

from .models import StateTransition, TaskRecord, TaskState

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE NOT NULL,
    step_name TEXT NOT NULL,
    payload TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    result TEXT,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    at TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    detail TEXT,
    FOREIGN KEY (task_id) REFERENCES tasks (task_id)
);
"""


class TaskStore:
    """Thread-safe wrapper around a SQLite database of task records."""

    def __init__(self, path: str = ":memory:"):
        # check_same_thread=False + an explicit lock: the FastAPI layer and
        # the engine's retry loop may touch this from different threads.
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def find_by_idempotency_key(self, key: str) -> Optional[TaskRecord]:
        with self._lock:
            row = self._conn.execute(
                "SELECT task_id FROM tasks WHERE idempotency_key = ?", (key,)
            ).fetchone()
        if row is None:
            return None
        return self.get(row[0])

    def create(self, record: TaskRecord) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT INTO tasks
                   (task_id, idempotency_key, step_name, payload, state,
                    attempt, max_attempts, result, error, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    record.task_id,
                    record.idempotency_key,
                    record.step_name,
                    json.dumps(record.payload),
                    record.state.value,
                    record.attempt,
                    record.max_attempts,
                    json.dumps(record.result) if record.result is not None else None,
                    record.error,
                    record.created_at.isoformat(),
                    record.updated_at.isoformat(),
                ),
            )
            for t in record.history:
                self._insert_transition(record.task_id, t)
            self._conn.commit()

    def save(self, record: TaskRecord, new_transition: Optional[StateTransition] = None) -> None:
        with self._lock:
            self._conn.execute(
                """UPDATE tasks SET state=?, attempt=?, result=?, error=?, updated_at=?
                   WHERE task_id=?""",
                (
                    record.state.value,
                    record.attempt,
                    json.dumps(record.result) if record.result is not None else None,
                    record.error,
                    record.updated_at.isoformat(),
                    record.task_id,
                ),
            )
            if new_transition is not None:
                self._insert_transition(record.task_id, new_transition)
            self._conn.commit()

    def _insert_transition(self, task_id: str, t: StateTransition) -> None:
        self._conn.execute(
            """INSERT INTO transitions (task_id, from_state, to_state, at, attempt, detail)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                task_id,
                t.from_state.value if t.from_state else None,
                t.to_state.value,
                t.at.isoformat(),
                t.attempt,
                t.detail,
            ),
        )

    def get(self, task_id: str) -> Optional[TaskRecord]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if row is None:
                return None
            cols = [d[0] for d in self._conn.execute("SELECT * FROM tasks LIMIT 0").description]
            data = dict(zip(cols, row))
            trows = self._conn.execute(
                "SELECT from_state, to_state, at, attempt, detail FROM transitions "
                "WHERE task_id = ? ORDER BY id ASC",
                (task_id,),
            ).fetchall()

        history = [
            StateTransition(
                from_state=TaskState(r[0]) if r[0] else None,
                to_state=TaskState(r[1]),
                at=datetime.fromisoformat(r[2]),
                attempt=r[3],
                detail=r[4] or "",
            )
            for r in trows
        ]
        return TaskRecord(
            task_id=data["task_id"],
            idempotency_key=data["idempotency_key"],
            step_name=data["step_name"],
            payload=json.loads(data["payload"]),
            state=TaskState(data["state"]),
            attempt=data["attempt"],
            max_attempts=data["max_attempts"],
            result=json.loads(data["result"]) if data["result"] is not None else None,
            error=data["error"],
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            history=history,
        )

    def list_all(self) -> list[TaskRecord]:
        # Order by rowid (SQLite's implicit insertion-order column) rather
        # than created_at: two tasks created in the same microsecond would
        # otherwise sort arbitrarily.
        with self._lock:
            ids = [r[0] for r in self._conn.execute("SELECT task_id FROM tasks ORDER BY rowid ASC")]
        return [self.get(i) for i in ids]
