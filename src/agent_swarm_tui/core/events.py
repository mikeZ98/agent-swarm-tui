"""Observable swarm events, emitted by the orchestrator to its listener."""

from typing import Final, Literal
from uuid import UUID

from pydantic import Field

from agent_swarm_tui.schemas import MAX_PAYLOAD_CHARS, AgentState, StrictModel, SwarmMessage

MAX_ERROR_CHARS: Final = 1024


class AgentUpdated(StrictModel):
    """An agent's state changed (status, task or usage counters)."""

    kind: Literal["agent_updated"] = "agent_updated"
    agent: AgentState


class MessageRouted(StrictModel):
    """A message was accepted by the router and enqueued for its recipients."""

    kind: Literal["message_routed"] = "message_routed"
    message: SwarmMessage
    recipients: tuple[UUID, ...]


class TaskCompleted(StrictModel):
    """An agent produced output with no further recipients: a swarm result."""

    kind: Literal["task_completed"] = "task_completed"
    agent_id: UUID
    output: str = Field(min_length=1, max_length=MAX_PAYLOAD_CHARS)


class AgentFailed(StrictModel):
    """An agent's runner or forwarding step failed; the agent is now ``ERROR``."""

    kind: Literal["agent_failed"] = "agent_failed"
    agent_id: UUID
    error: str = Field(min_length=1, max_length=MAX_ERROR_CHARS)


type SwarmEvent = AgentUpdated | MessageRouted | TaskCompleted | AgentFailed
