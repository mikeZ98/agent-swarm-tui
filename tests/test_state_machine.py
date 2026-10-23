"""Agent lifecycle: the transition table, task carry-over rules and pure updates."""

import itertools
import math

import pytest

from agent_swarm_tui.core import (
    TRANSITIONS,
    InvalidStateTransitionError,
    can_transition,
    record_usage,
    transition,
)
from agent_swarm_tui.schemas import AgentRole, AgentState, AgentStatus

S = AgentStatus
LEGAL = {
    (S.IDLE, S.RUNNING),
    (S.IDLE, S.TERMINATED),
    (S.RUNNING, S.WAITING),
    (S.RUNNING, S.IDLE),
    (S.RUNNING, S.ERROR),
    (S.RUNNING, S.TERMINATED),
    (S.WAITING, S.RUNNING),
    (S.WAITING, S.ERROR),
    (S.WAITING, S.TERMINATED),
    (S.ERROR, S.IDLE),
    (S.ERROR, S.RUNNING),
    (S.ERROR, S.TERMINATED),
}


def in_status(status: AgentStatus) -> AgentState:
    """An agent in ``status`` (with a task where the status needs or allows one)."""
    task = None if status in (S.IDLE, S.TERMINATED) else "task"
    return AgentState(name="a", role=AgentRole.EXECUTOR, status=status, current_task=task)


def test_table_matches_specification() -> None:
    assert {(src, dst) for src, dsts in TRANSITIONS.items() for dst in dsts} == LEGAL
    assert set(TRANSITIONS) == set(S)


def test_table_is_read_only() -> None:
    with pytest.raises(TypeError):
        TRANSITIONS[S.IDLE] = frozenset()  # type: ignore[index]


@pytest.mark.parametrize(("source", "target"), list(itertools.product(S, S)))
def test_every_pair(source: AgentStatus, target: AgentStatus) -> None:
    state = in_status(source)
    assert can_transition(source, target) is ((source, target) in LEGAL)
    if (source, target) in LEGAL:
        task = "new" if source is S.IDLE and target is S.RUNNING else None
        moved = transition(state, target, task=task)
        assert moved.status is target
        assert moved.id == state.id
        assert state.status is source  # input untouched
    else:
        with pytest.raises(InvalidStateTransitionError) as caught:
            transition(state, target, task="x")
        assert (caught.value.source, caught.value.target) == (source, target)
        assert caught.value.agent_id == state.id
        assert caught.value.reason is None


def test_terminated_is_final() -> None:
    assert not TRANSITIONS[S.TERMINATED]


class TestTaskRules:
    def test_start_requires_a_task(self) -> None:
        with pytest.raises(InvalidStateTransitionError, match="requires a current_task") as caught:
            transition(in_status(S.IDLE), S.RUNNING)
        assert caught.value.reason
        assert caught.value.__cause__ is not None

    def test_waiting_and_resume_keep_the_task(self) -> None:
        running = transition(in_status(S.IDLE), S.RUNNING, task="t1")
        waiting = transition(running, S.WAITING)
        assert waiting.current_task == "t1"
        assert transition(waiting, S.RUNNING).current_task == "t1"

    def test_retry_after_error_keeps_or_replaces_the_task(self) -> None:
        failed = transition(transition(in_status(S.IDLE), S.RUNNING, task="t1"), S.ERROR)
        assert transition(failed, S.RUNNING).current_task == "t1"
        assert transition(failed, S.RUNNING, task="t2").current_task == "t2"

    @pytest.mark.parametrize("target", [S.IDLE, S.TERMINATED])
    def test_idle_and_terminated_clear_the_task(self, target: AgentStatus) -> None:
        assert transition(in_status(S.RUNNING), target).current_task is None

    def test_explicit_task_on_idle_is_rejected(self) -> None:
        with pytest.raises(InvalidStateTransitionError, match="must not carry"):
            transition(in_status(S.RUNNING), S.IDLE, task="x")

    def test_error_message_names_the_transition(self) -> None:
        with pytest.raises(InvalidStateTransitionError, match="idle -> waiting"):
            transition(in_status(S.IDLE), S.WAITING)


class TestRecordUsage:
    def test_accumulates_tokens_and_keeps_latest_latency(self) -> None:
        state = in_status(S.RUNNING)
        updated = record_usage(
            record_usage(state, tokens=10, latency_ms=5.0), tokens=5, latency_ms=7
        )
        assert (updated.total_tokens, updated.latency_ms, updated.status) == (15, 7.0, S.RUNNING)
        assert state.total_tokens == 0

    def test_rejects_negative_tokens(self) -> None:
        with pytest.raises(ValueError, match=">= 0"):
            record_usage(in_status(S.RUNNING), tokens=-1, latency_ms=1.0)

    @pytest.mark.parametrize("latency", [math.nan, math.inf, -1.0])
    def test_rejects_bad_latency(self, latency: float) -> None:
        with pytest.raises(ValueError, match="latency_ms"):
            record_usage(in_status(S.RUNNING), tokens=0, latency_ms=latency)
