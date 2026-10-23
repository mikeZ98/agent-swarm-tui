"""Domain exception hierarchy.

Core components raise only these, so callers (and the TUI) never have to
handle asyncio or Pydantic internals for expected failures.
"""

from uuid import UUID

from agent_swarm_tui.schemas import AgentStatus


class SwarmError(Exception):
    """Base class for all swarm domain errors."""


class InvalidStateTransitionError(SwarmError):
    """An agent was asked to move to a status its current status cannot reach."""

    def __init__(
        self,
        agent_id: UUID,
        source: AgentStatus,
        target: AgentStatus,
        *,
        reason: str | None = None,
    ) -> None:
        """Attach the rejected transition to the error."""
        detail = f"agent {agent_id}: illegal transition {source.value} -> {target.value}"
        super().__init__(f"{detail} ({reason})" if reason else detail)
        self.agent_id = agent_id
        self.source = source
        self.target = target
        self.reason = reason


class RoutingError(SwarmError):
    """A message violates the swarm's routing strategy or cannot be delivered."""


class AgentNotFoundError(SwarmError, LookupError):
    """An agent id does not belong to the swarm."""

    def __init__(self, agent_id: UUID) -> None:
        """Attach the unknown agent id to the error."""
        super().__init__(f"agent {agent_id} is not part of the swarm")
        self.agent_id = agent_id
