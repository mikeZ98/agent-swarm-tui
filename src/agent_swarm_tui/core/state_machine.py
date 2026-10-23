"""Deterministic agent lifecycle: a fixed transition table over ``AgentStatus``.

Every function is pure: it returns a new, fully re-validated ``AgentState``
(via ``AgentState.model_validate``) and never mutates its input.

::

    IDLE ──> RUNNING ──> IDLE                 (task completed)
               │ ├─────> WAITING ──> RUNNING  (blocked, then resumed)
               │ └─────> ERROR ──> RUNNING    (retry) | IDLE (reset)
    any non-terminal ──> TERMINATED           (final)
"""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Final

from pydantic import ValidationError

from agent_swarm_tui.core.errors import InvalidStateTransitionError
from agent_swarm_tui.schemas import AgentState, AgentStatus

TRANSITIONS: Final[Mapping[AgentStatus, frozenset[AgentStatus]]] = MappingProxyType(
    {
        AgentStatus.IDLE: frozenset({AgentStatus.RUNNING, AgentStatus.TERMINATED}),
        AgentStatus.RUNNING: frozenset(
            {AgentStatus.WAITING, AgentStatus.IDLE, AgentStatus.ERROR, AgentStatus.TERMINATED}
        ),
        AgentStatus.WAITING: frozenset(
            {AgentStatus.RUNNING, AgentStatus.ERROR, AgentStatus.TERMINATED}
        ),
        AgentStatus.ERROR: frozenset(
            {AgentStatus.IDLE, AgentStatus.RUNNING, AgentStatus.TERMINATED}
        ),
        AgentStatus.TERMINATED: frozenset(),
    }
)

_CLEARS_TASK: Final = frozenset({AgentStatus.IDLE, AgentStatus.TERMINATED})


def can_transition(source: AgentStatus, target: AgentStatus) -> bool:
    """Return whether ``source -> target`` is a legal transition."""
    return target in TRANSITIONS[source]


def transition(state: AgentState, target: AgentStatus, *, task: str | None = None) -> AgentState:
    """Move ``state`` to ``target``, returning the new state.

    ``task`` replaces ``current_task``. When omitted, the task is cleared for
    ``IDLE``/``TERMINATED`` and carried over otherwise (so ``WAITING -> RUNNING``
    resumes, and ``ERROR -> RUNNING`` retries, the same task).

    Raises ``InvalidStateTransitionError`` for transitions outside
    ``TRANSITIONS`` and for results that violate the ``AgentState`` contract
    (e.g. ``RUNNING`` without a task).
    """
    if not can_transition(state.status, target):
        raise InvalidStateTransitionError(state.id, state.status, target)
    if task is None:
        task = None if target in _CLEARS_TASK else state.current_task
    try:
        return _revalidate(state, status=target, current_task=task)
    except ValidationError as exc:
        reason = "; ".join(str(error["msg"]) for error in exc.errors())
        raise InvalidStateTransitionError(state.id, state.status, target, reason=reason) from exc


def record_usage(state: AgentState, *, tokens: int, latency_ms: float) -> AgentState:
    """Add ``tokens`` to the running total and store the latest ``latency_ms``.

    The status is unchanged. Raises ``ValueError`` for negative or non-finite inputs.
    """
    if tokens < 0:
        raise ValueError(f"tokens must be >= 0, got {tokens}")
    return _revalidate(state, total_tokens=state.total_tokens + tokens, latency_ms=latency_ms)


def _revalidate(state: AgentState, **changes: Any) -> AgentState:  # noqa: ANN401
    return AgentState.model_validate({**state.model_dump(), **changes})
