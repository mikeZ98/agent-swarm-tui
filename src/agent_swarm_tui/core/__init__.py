"""Swarm core: agent state machine, message routing and orchestration."""

from agent_swarm_tui.core.errors import (
    AgentNotFoundError,
    InvalidStateTransitionError,
    RoutingError,
    SwarmError,
)
from agent_swarm_tui.core.events import (
    AgentFailed,
    AgentUpdated,
    MessageRouted,
    SwarmEvent,
    TaskCompleted,
)
from agent_swarm_tui.core.orchestrator import (
    DEFAULT_DRAIN_TIMEOUT,
    SwarmListener,
    SwarmOrchestrator,
)
from agent_swarm_tui.core.ports import AgentReply, AgentRunner
from agent_swarm_tui.core.router import DEFAULT_MAILBOX_SIZE, MessageRouter
from agent_swarm_tui.core.state_machine import (
    TRANSITIONS,
    can_transition,
    record_usage,
    transition,
)

__all__ = [
    "DEFAULT_DRAIN_TIMEOUT",
    "DEFAULT_MAILBOX_SIZE",
    "TRANSITIONS",
    "AgentFailed",
    "AgentNotFoundError",
    "AgentReply",
    "AgentRunner",
    "AgentUpdated",
    "InvalidStateTransitionError",
    "MessageRouted",
    "MessageRouter",
    "RoutingError",
    "SwarmError",
    "SwarmEvent",
    "SwarmListener",
    "SwarmOrchestrator",
    "TaskCompleted",
    "can_transition",
    "record_usage",
    "transition",
]
