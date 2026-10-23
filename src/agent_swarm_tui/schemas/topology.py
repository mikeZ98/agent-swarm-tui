"""Swarm composition and routing contracts."""

from enum import StrEnum
from typing import Annotated, Final, Self
from uuid import UUID, uuid4

from pydantic import Field, model_validator

from agent_swarm_tui.schemas.agent import AGENT_NAME_PATTERN, AgentRole, AgentState
from agent_swarm_tui.schemas.base import StrictModel

MAX_AGENTS: Final = 64


class RoutingStrategy(StrEnum):
    """How work flows between the agents of a swarm."""

    SEQUENTIAL = "sequential"
    """Pipeline: each agent hands its output to the next, in declaration order."""
    HIERARCHICAL = "hierarchical"
    """A single orchestrator delegates to, and collects from, worker agents."""
    MESH = "mesh"
    """Peer-to-peer: any agent may message any other."""


AgentRoster = Annotated[tuple[AgentState, ...], Field(min_length=1, max_length=MAX_AGENTS)]


class SwarmTopology(StrictModel):
    """A named swarm: its agents (ordered, immutable) and routing strategy.

    Agent ids and names are unique; ``HIERARCHICAL`` requires exactly one
    ``ORCHESTRATOR``.
    """

    swarm_id: UUID = Field(default_factory=uuid4)
    name: str = Field(min_length=1, max_length=64, pattern=AGENT_NAME_PATTERN)
    agents: AgentRoster
    routing_strategy: RoutingStrategy

    @model_validator(mode="after")
    def _roster_is_consistent(self) -> Self:
        ids = [agent.id for agent in self.agents]
        if len(set(ids)) != len(ids):
            raise ValueError("agent ids must be unique within a swarm")
        names = [agent.name.casefold() for agent in self.agents]
        if len(set(names)) != len(names):
            raise ValueError("agent names must be unique (case-insensitive) within a swarm")
        if self.routing_strategy is RoutingStrategy.HIERARCHICAL:
            orchestrators = sum(agent.role is AgentRole.ORCHESTRATOR for agent in self.agents)
            if orchestrators != 1:
                raise ValueError(
                    f"hierarchical routing requires exactly one orchestrator, got {orchestrators}"
                )
        return self

    def agent(self, agent_id: UUID) -> AgentState:
        """Return the agent with ``agent_id``; raise ``KeyError`` if absent."""
        for candidate in self.agents:
            if candidate.id == agent_id:
                return candidate
        raise KeyError(agent_id)
