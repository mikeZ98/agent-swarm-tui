"""Interfaces the orchestrator depends on; concrete agent runners live outside ``core``."""

from collections.abc import Callable
from typing import Protocol, Self, runtime_checkable
from uuid import UUID

from pydantic import Field, model_validator

from agent_swarm_tui.schemas import (
    MAX_AGENTS,
    MAX_PAYLOAD_CHARS,
    AgentState,
    AgentStatus,
    StrictModel,
    SwarmMessage,
)

type StatusHook = Callable[[UUID, AgentStatus], None]
"""Moves an agent between ``RUNNING`` and ``WAITING`` while its message is in flight."""


class AgentReply(StrictModel):
    """What an agent produced for one inbound message.

    ``output`` is forwarded as follows:

    - ``broadcast=True``: one ``BROADCAST`` message.
    - ``forward_to`` set: one ``DIRECT`` message per id (``()`` = do not forward).
    - neither: the routing strategy's default next hop (see
      ``MessageRouter.default_targets``).

    Output with no recipients is a final result for the swarm.
    """

    tokens_used: int = Field(default=0, ge=0)
    output: str | None = Field(default=None, min_length=1, max_length=MAX_PAYLOAD_CHARS)
    forward_to: tuple[UUID, ...] | None = Field(default=None, max_length=MAX_AGENTS)
    broadcast: bool = False

    @model_validator(mode="after")
    def _forwarding_is_coherent(self) -> Self:
        if self.output is None and (self.broadcast or self.forward_to):
            raise ValueError("forward_to/broadcast require an output")
        if self.broadcast and self.forward_to is not None:
            raise ValueError("broadcast and forward_to are mutually exclusive")
        if self.forward_to and len(set(self.forward_to)) != len(self.forward_to):
            raise ValueError("forward_to must not contain duplicates")
        return self


class AgentRunner(Protocol):
    """Executes one agent's work for one message.

    Any ``Exception`` marks the agent ``ERROR`` (the swarm keeps running);
    implementations must let ``asyncio.CancelledError`` propagate.
    """

    async def run(self, agent: AgentState, message: SwarmMessage) -> AgentReply:
        """Handle ``message`` as ``agent`` (whose status is ``RUNNING``)."""
        ...


@runtime_checkable
class StatusAwareRunner(Protocol):
    """Optional runner capability: report ``WAITING`` / ``RUNNING`` mid-message.

    ``SwarmOrchestrator`` binds its ``signal_status`` automatically to any
    runner that implements this. The hook raises ``SwarmError`` subclasses
    when the agent is not currently handling a message.
    """

    def bind_status_hook(self, hook: StatusHook) -> None:
        """Receive the hook used to signal status changes."""
        ...
