"""Ready-made swarm topologies for the CLI (one per routing strategy)."""

from agent_swarm_tui.client import ReplyFactory, delegating_reply, next_hop_reply
from agent_swarm_tui.schemas import AgentRole, AgentState, RoutingStrategy, SwarmTopology

_WORKERS = (
    ("researcher", AgentRole.RESEARCHER),
    ("builder", AgentRole.EXECUTOR),
    ("reviewer", AgentRole.REVIEWER),
)


def build_topology(strategy: RoutingStrategy, *, name: str = "swarm") -> SwarmTopology:
    """Four-agent swarm for ``strategy``.

    ``SEQUENTIAL``: planner -> researcher -> builder -> reviewer pipeline.
    ``HIERARCHICAL``: a ``lead`` orchestrator over the three workers.
    ``MESH``: the same four agents as peers.
    """
    head = (
        AgentState(name="lead", role=AgentRole.ORCHESTRATOR)
        if strategy is RoutingStrategy.HIERARCHICAL
        else AgentState(name="planner", role=AgentRole.PLANNER)
    )
    workers = tuple(AgentState(name=n, role=role) for n, role in _WORKERS)
    return SwarmTopology(name=name, agents=(head, *workers), routing_strategy=strategy)


def reply_factory_for(strategy: RoutingStrategy) -> ReplyFactory:
    """Reply routing that makes ``strategy`` useful out of the box.

    ``HIERARCHICAL`` needs the orchestrator to broadcast its plan to the
    workers (``delegating_reply``); the others follow the default next hop.
    """
    return delegating_reply if strategy is RoutingStrategy.HIERARCHICAL else next_hop_reply
