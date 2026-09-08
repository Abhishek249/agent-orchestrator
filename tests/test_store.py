from __future__ import annotations

from orchestrator.models import TaskRecord, TaskState
from orchestrator.store import TaskStore


def test_create_and_get_round_trips_all_fields():
    store = TaskStore()
    record = TaskRecord(
        task_id="t1",
        idempotency_key="k1",
        step_name="research",
        payload={"topic": "orchestration"},
        max_attempts=5,
    )
    record.record_transition(TaskState.PENDING, detail="submitted")
    store.create(record)

    fetched = store.get("t1")
    assert fetched is not None
    assert fetched.task_id == "t1"
    assert fetched.payload == {"topic": "orchestration"}
    assert fetched.max_attempts == 5
    assert len(fetched.history) == 1
    assert fetched.history[0].to_state == TaskState.PENDING


def test_find_by_idempotency_key_returns_none_when_absent():
    store = TaskStore()
    assert store.find_by_idempotency_key("does-not-exist") is None


def test_save_appends_transition_and_persists_result():
    store = TaskStore()
    record = TaskRecord(task_id="t2", idempotency_key="k2", step_name="draft", payload={})
    record.record_transition(TaskState.PENDING)
    store.create(record)

    record.result = {"draft": "hello"}
    record.record_transition(TaskState.SUCCEEDED, detail="done")
    store.save(record, record.history[-1])

    fetched = store.get("t2")
    assert fetched.result == {"draft": "hello"}
    assert fetched.state == TaskState.SUCCEEDED
    assert [t.to_state for t in fetched.history] == [TaskState.PENDING, TaskState.SUCCEEDED]


def test_idempotency_key_is_unique_at_the_schema_level():
    import sqlite3

    import pytest

    store = TaskStore()
    r1 = TaskRecord(task_id="a", idempotency_key="dupe", step_name="s", payload={})
    r1.record_transition(TaskState.PENDING)
    store.create(r1)

    r2 = TaskRecord(task_id="b", idempotency_key="dupe", step_name="s", payload={})
    r2.record_transition(TaskState.PENDING)
    with pytest.raises(sqlite3.IntegrityError):
        store.create(r2)


def test_list_all_returns_tasks_in_creation_order():
    store = TaskStore()
    for i in range(3):
        r = TaskRecord(task_id=f"t{i}", idempotency_key=f"k{i}", step_name="s", payload={})
        r.record_transition(TaskState.PENDING)
        store.create(r)

    all_tasks = store.list_all()
    assert [t.task_id for t in all_tasks] == ["t0", "t1", "t2"]
