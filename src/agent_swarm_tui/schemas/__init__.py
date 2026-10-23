"""Strict Pydantic v2 domain models and state contracts for the swarm."""

from agent_swarm_tui.schemas.agent import (
    AGENT_NAME_PATTERN,
    MAX_TASK_CHARS,
    AgentRole,
    AgentState,
    AgentStatus,
)
from agent_swarm_tui.schemas.base import StrictModel
from agent_swarm_tui.schemas.message import MAX_PAYLOAD_CHARS, MessageType, SwarmMessage
from agent_swarm_tui.schemas.metrics import SwarmMetrics
from agent_swarm_tui.schemas.topology import (
    MAX_AGENTS,
    AgentRoster,
    RoutingStrategy,
    SwarmTopology,
)

__all__ = [
    "AGENT_NAME_PATTERN",
    "MAX_AGENTS",
    "MAX_PAYLOAD_CHARS",
    "MAX_TASK_CHARS",
    "AgentRole",
    "AgentRoster",
    "AgentState",
    "AgentStatus",
    "MessageType",
    "RoutingStrategy",
    "StrictModel",
    "SwarmMessage",
    "SwarmMetrics",
    "SwarmTopology",
]
