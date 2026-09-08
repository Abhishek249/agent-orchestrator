from __future__ import annotations

import pytest

from orchestrator import DeadLetterError, Orchestrator, RetryPolicy, TaskState


def make_orchestrator() -> Orchestrator:
    # sleep_fn is a no-op so the retry/backoff tests run instantly instead
    # of actually sleeping - the backoff *math* is still exercised via
    # RetryPolicy.delay_for, just not the wall-clock wait.
    return Orchestrator(sleep_fn=lambda seconds: None)


def test_successful_task_reaches_succeeded_with_full_history():
    orch = make_orchestrator()
    orch.register_step("echo", lambda payload: {"echoed": payload["value"]})

    record = orch.submit_and_run("echo", {"value": 42}, idempotency_key="echo:42")

    assert record.state == TaskState.SUCCEEDED
    assert record.result == {"echoed": 42}
    assert record.attempt == 1
    states = [t.to_state for t in record.history]
    assert states == [TaskState.PENDING, TaskState.RUNNING, TaskState.SUCCEEDED]


def test_idempotent_submission_does_not_rerun_the_step():
    orch = make_orchestrator()
    calls = {"count": 0}

    def side_effecting_step(payload):
        calls["count"] += 1
        return {"ran": calls["count"]}

    orch.register_step("bill_customer", side_effecting_step)

    first = orch.submit_and_run("bill_customer", {"amount": 100}, idempotency_key="invoice-123")
    second = orch.submit_and_run("bill_customer", {"amount": 100}, idempotency_key="invoice-123")

    assert first.task_id == second.task_id
    assert calls["count"] == 1, "the side-effecting step must not run twice for the same idempotency key"
    assert second.result == {"ran": 1}


def test_retries_transient_failures_and_eventually_succeeds():
    orch = make_orchestrator()
    attempts = {"n": 0}

    def flaky(payload):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("simulated transient failure")
        return {"ok": True}

    orch.register_step("flaky_call", flaky)
    policy = RetryPolicy(max_attempts=5, base_delay_s=0.001, max_delay_s=0.001)

    record = orch.submit_and_run("flaky_call", {}, idempotency_key="k1", retry_policy=policy)

    assert record.state == TaskState.SUCCEEDED
    assert attempts["n"] == 3
    assert record.attempt == 3
    retrying_transitions = [t for t in record.history if t.to_state == TaskState.RETRYING]
    assert len(retrying_transitions) == 2


def test_exhausting_retries_moves_task_to_dead_letter():
    orch = make_orchestrator()

    def always_fails(payload):
        raise RuntimeError("permanently broken dependency")

    orch.register_step("doomed", always_fails)
    policy = RetryPolicy(max_attempts=3, base_delay_s=0.001, max_delay_s=0.001)

    # submit_and_run() catches DeadLetterError and returns the failed record
    # so callers can inspect it directly; run() itself still raises, which
    # is asserted separately below on a fresh task.
    record = orch.submit_and_run("doomed", {}, idempotency_key="k2", retry_policy=policy)
    assert record.state == TaskState.DEAD_LETTER
    assert record.attempt == 3
    assert "permanently broken dependency" in (record.error or "")

    second = orch.submit("doomed", {}, idempotency_key="k2-raises", retry_policy=policy)
    with pytest.raises(DeadLetterError):
        orch.run(second.task_id, retry_policy=policy)


def test_dead_letter_task_is_not_retried_again_on_rerun():
    orch = make_orchestrator()
    call_count = {"n": 0}

    def always_fails(payload):
        call_count["n"] += 1
        raise RuntimeError("still broken")

    orch.register_step("doomed", always_fails)
    policy = RetryPolicy(max_attempts=2, base_delay_s=0.001, max_delay_s=0.001)

    record = orch.submit_and_run("doomed", {}, idempotency_key="k3", retry_policy=policy)
    assert record.state == TaskState.DEAD_LETTER
    calls_after_first_run = call_count["n"]

    # Calling run() again on an already dead-lettered task must be a no-op:
    # a converged terminal state should never re-execute the step.
    record_again = orch.run(record.task_id)
    assert record_again.state == TaskState.DEAD_LETTER
    assert call_count["n"] == calls_after_first_run


def test_different_idempotency_keys_run_independently():
    orch = make_orchestrator()
    orch.register_step("noop", lambda payload: payload)

    a = orch.submit_and_run("noop", {"x": 1}, idempotency_key="a")
    b = orch.submit_and_run("noop", {"x": 2}, idempotency_key="b")

    assert a.task_id != b.task_id
    assert a.result == {"x": 1}
    assert b.result == {"x": 2}


def test_retry_policy_backoff_is_bounded_and_nonnegative():
    policy = RetryPolicy(max_attempts=6, base_delay_s=0.1, max_delay_s=1.0, jitter=False)
    delays = [policy.delay_for(attempt) for attempt in range(1, 7)]
    assert all(d >= 0 for d in delays)
    assert all(d <= policy.max_delay_s for d in delays)
    # Exponential growth until it saturates at the ceiling.
    assert delays[0] < delays[1] < delays[2]
    assert delays[-1] == pytest.approx(policy.max_delay_s)
