"""Agent identity and runtime-state contracts."""

from enum import StrEnum
from typing import Final, Self
from uuid import UUID, uuid4

from pydantic import Field, FiniteFloat, model_validator

from agent_swarm_tui.schemas.base import StrictModel

AGENT_NAME_PATTERN: Final = r"^[A-Za-z0-9][A-Za-z0-9 _.-]*$"
MAX_TASK_CHARS: Final = 4096


class AgentStatus(StrEnum):
    """Lifecycle states of a single agent."""

    IDLE = "idle"
    """Alive, no task assigned."""
    RUNNING = "running"
    """Actively working on ``current_task``."""
    WAITING = "waiting"
    """Blocked on a message, tool result or upstream response."""
    ERROR = "error"
    """Last task failed; may be retried or terminated."""
    TERMINATED = "terminated"
    """Final state; the agent accepts no further work."""


class AgentRole(StrEnum):
    """Responsibility an agent holds within the swarm."""

    ORCHESTRATOR = "orchestrator"
    PLANNER = "planner"
    RESEARCHER = "researcher"
    EXECUTOR = "executor"
    REVIEWER = "reviewer"


_TASK_REQUIRED: Final = frozenset({AgentStatus.RUNNING})
_TASK_FORBIDDEN: Final = frozenset({AgentStatus.IDLE, AgentStatus.TERMINATED})


class AgentState(StrictModel):
    """Point-in-time snapshot of one agent; transitions produce a new instance.

    ``RUNNING`` requires a ``current_task``; ``IDLE`` and ``TERMINATED`` forbid one.
    """

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(min_length=1, max_length=64, pattern=AGENT_NAME_PATTERN)
    role: AgentRole
    status: AgentStatus = AgentStatus.IDLE
    current_task: str | None = Field(default=None, min_length=1, max_length=MAX_TASK_CHARS)
    total_tokens: int = Field(default=0, ge=0)
    latency_ms: FiniteFloat = Field(default=0.0, ge=0.0)

    @model_validator(mode="after")
    def _task_matches_status(self) -> Self:
        if self.status in _TASK_REQUIRED and self.current_task is None:
            raise ValueError(f"status {self.status.value!r} requires a current_task")
        if self.status in _TASK_FORBIDDEN and self.current_task is not None:
            raise ValueError(f"status {self.status.value!r} must not carry a current_task")
        return self
