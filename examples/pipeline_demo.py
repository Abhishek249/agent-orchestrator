"""A small multi-agent pipeline demonstrating the orchestrator end to end.

Simulates a "research -> draft -> review -> publish" agent workflow where
the review step is deliberately flaky (fails the first two times it sees a
given input) so you can watch retries, backoff, and eventual success play
out in the state history. Run with:

    python -m examples.pipeline_demo
"""
from __future__ import annotations

import random

from orchestrator import Orchestrator, RetryPolicy, DeadLetterError, TaskState

# Simulates a transient dependency (e.g. a flaky external review API) that
# fails deterministically twice per distinct input, then succeeds - this is
# what makes the retry path exercise for real instead of always succeeding
# on attempt 1.
_review_failure_counts: dict[str, int] = {}


def research(payload: dict) -> dict:
    topic = payload["topic"]
    return {"topic": topic, "notes": f"three key facts about {topic}"}


def draft(payload: dict) -> dict:
    return {"draft": f"Draft based on: {payload['notes']}"}


def review(payload: dict) -> dict:
    key = payload["draft"]
    seen = _review_failure_counts.get(key, 0)
    _review_failure_counts[key] = seen + 1
    if seen < 2:
        raise RuntimeError("review service timed out (simulated transient failure)")
    return {"approved_draft": payload["draft"], "review_attempts": seen + 1}


def publish(payload: dict) -> dict:
    return {"published": True, "content": payload["approved_draft"]}


def run_pipeline(topic: str) -> None:
    orch = Orchestrator()
    for name, fn in [("research", research), ("draft", draft), ("review", review), ("publish", publish)]:
        orch.register_step(name, fn)

    policy = RetryPolicy(max_attempts=4, base_delay_s=0.01, max_delay_s=0.05)

    state = {"topic": topic}

    t1 = orch.submit_and_run("research", state, idempotency_key=f"{topic}:research", retry_policy=policy)
    print(f"[research] {t1.state.value} -> {t1.result}")

    t2 = orch.submit_and_run("draft", t1.result, idempotency_key=f"{topic}:draft", retry_policy=policy)
    print(f"[draft]    {t2.state.value} -> {t2.result}")

    t3 = orch.submit_and_run("review", t2.result, idempotency_key=f"{topic}:review", retry_policy=policy)
    print(f"[review]   {t3.state.value} -> {t3.result}")
    print("  state history for the review task (this is the audit trail an operator or another agent can query):")
    for h in t3.history:
        frm = h.from_state.value if h.from_state else "-"
        print(f"    {frm:>10} -> {h.to_state.value:<10} attempt={h.attempt}  {h.detail}")

    if t3.state != TaskState.SUCCEEDED:
        print("review did not converge; stopping pipeline instead of publishing bad input")
        return

    t4 = orch.submit_and_run("publish", t3.result, idempotency_key=f"{topic}:publish", retry_policy=policy)
    print(f"[publish]  {t4.state.value} -> {t4.result}")

    # Re-submitting the exact same logical work is safe: same idempotency
    # key, same record, no duplicate publish.
    t4_again = orch.submit_and_run("publish", t3.result, idempotency_key=f"{topic}:publish", retry_policy=policy)
    assert t4_again.task_id == t4.task_id
    print("re-submission returned the same task_id - no duplicate publish occurred.")


if __name__ == "__main__":
    random.seed(7)
    run_pipeline("agent orchestration for SDLC automation")
