"""Per-agent mailboxes and topology-enforcing message routing.

Routing policy by strategy (``SYSTEM`` messages bypass it: they come from the
runtime and reach one agent, or all agents when untargeted):

=============  ==============================  ==============================
Strategy       ``DIRECT``                      ``BROADCAST``
=============  ==============================  ==============================
SEQUENTIAL     only to the next pipeline step  rejected
HIERARCHICAL   orchestrator <-> worker only    orchestrator only, to workers
MESH           any peer                        any agent, to all peers
=============  ==============================  ==============================
"""

import asyncio
from collections import Counter
from collections.abc import Sequence
from typing import Final, assert_never
from uuid import UUID

from agent_swarm_tui.core.errors import AgentNotFoundError, RoutingError
from agent_swarm_tui.schemas import (
    AgentRole,
    MessageType,
    RoutingStrategy,
    SwarmMessage,
    SwarmTopology,
)

DEFAULT_MAILBOX_SIZE: Final = 1024


class MessageRouter:
    """Owns one bounded ``asyncio.Queue`` mailbox per agent of a topology.

    Delivery never awaits: ``route_batch`` validates every message and checks
    mailbox capacity first, then enqueues all of them, or raises and enqueues
    none. A full mailbox is a ``RoutingError`` rather than backpressure, so two
    agents messaging each other can never deadlock.
    """

    def __init__(
        self, topology: SwarmTopology, *, mailbox_size: int = DEFAULT_MAILBOX_SIZE
    ) -> None:
        """Create empty mailboxes for every agent in ``topology``."""
        if mailbox_size < 1:
            raise ValueError(f"mailbox_size must be >= 1, got {mailbox_size}")
        self._topology = topology
        self._order = tuple(agent.id for agent in topology.agents)
        self._index = {agent_id: position for position, agent_id in enumerate(self._order)}
        self._names = {agent.id: agent.name for agent in topology.agents}
        self._hub = (
            next(a.id for a in topology.agents if a.role is AgentRole.ORCHESTRATOR)
            if topology.routing_strategy is RoutingStrategy.HIERARCHICAL
            else None
        )
        self._mailboxes: dict[UUID, asyncio.Queue[SwarmMessage]] = {
            agent_id: asyncio.Queue(maxsize=mailbox_size) for agent_id in self._order
        }
        self._closed = False

    @property
    def topology(self) -> SwarmTopology:
        """The topology whose policy this router enforces."""
        return self._topology

    @property
    def entry_point(self) -> UUID:
        """Agent that receives dispatched tasks by default.

        The orchestrator for ``HIERARCHICAL``, otherwise the first agent.
        """
        return self._hub if self._hub is not None else self._order[0]

    @property
    def is_closed(self) -> bool:
        """Whether ``shutdown`` has been called."""
        return self._closed

    def mailbox(self, agent_id: UUID) -> asyncio.Queue[SwarmMessage]:
        """Return the inbound queue of ``agent_id``."""
        self._require_agent(agent_id)
        return self._mailboxes[agent_id]

    def successor(self, agent_id: UUID) -> UUID | None:
        """Return the agent after ``agent_id`` in declaration order, if any."""
        self._require_agent(agent_id)
        position = self._index[agent_id] + 1
        return self._order[position] if position < len(self._order) else None

    def default_targets(self, sender_id: UUID) -> tuple[UUID, ...]:
        """Default next hop for an agent's output under the routing strategy.

        ``SEQUENTIAL``: the successor (none for the last step). ``HIERARCHICAL``:
        workers report to the orchestrator, which must delegate explicitly.
        ``MESH``: none (peers address each other explicitly).
        """
        self._require_agent(sender_id)
        strategy = self._topology.routing_strategy
        match strategy:
            case RoutingStrategy.SEQUENTIAL:
                successor = self.successor(sender_id)
                return () if successor is None else (successor,)
            case RoutingStrategy.HIERARCHICAL:
                return () if sender_id == self._hub or self._hub is None else (self._hub,)
            case RoutingStrategy.MESH:
                return ()
            case _:
                assert_never(strategy)

    def recipients(self, message: SwarmMessage) -> tuple[UUID, ...]:
        """Resolve who receives ``message``, enforcing the routing strategy.

        Raises ``AgentNotFoundError`` for unknown agents and ``RoutingError``
        for messages the strategy forbids. Pure: enqueues nothing.
        """
        if message.message_type is MessageType.SYSTEM:
            return self._system_recipients(message)
        self._require_agent(message.sender_id)
        if message.message_type is MessageType.BROADCAST:
            return self._broadcast_recipients(message.sender_id)
        if message.target_id is None:  # pragma: no cover - guaranteed by SwarmMessage
            raise RoutingError("direct message without target_id")
        self._require_agent(message.target_id)
        self._check_direct(message.sender_id, message.target_id)
        return (message.target_id,)

    def route(self, message: SwarmMessage) -> tuple[UUID, ...]:
        """Deliver one message; return its recipients."""
        return self.route_batch((message,))[0]

    def route_batch(self, messages: Sequence[SwarmMessage]) -> tuple[tuple[UUID, ...], ...]:
        """Deliver ``messages`` atomically; return each message's recipients."""
        if self._closed:
            raise RoutingError("router is shut down")
        plan = tuple(self.recipients(message) for message in messages)
        load = Counter(agent_id for recipients in plan for agent_id in recipients)
        for agent_id, incoming in load.items():
            mailbox = self._mailboxes[agent_id]
            if mailbox.qsize() + incoming > mailbox.maxsize:
                raise RoutingError(f"mailbox of agent {self._label(agent_id)} is full")
        for message, recipients in zip(messages, plan, strict=True):
            for agent_id in recipients:
                self._mailboxes[agent_id].put_nowait(message)
        return plan

    def shutdown(self, *, immediate: bool = False) -> None:
        """Close every mailbox (idempotent).

        Consumers drain what is queued and then get ``asyncio.QueueShutDown``;
        with ``immediate=True`` queued messages are discarded.
        """
        self._closed = True
        for mailbox in self._mailboxes.values():
            mailbox.shutdown(immediate=immediate)

    def _system_recipients(self, message: SwarmMessage) -> tuple[UUID, ...]:
        if message.sender_id != self._topology.swarm_id:
            raise RoutingError("system messages may only originate from the swarm runtime")
        if message.target_id is None:
            return self._order
        self._require_agent(message.target_id)
        return (message.target_id,)

    def _broadcast_recipients(self, sender_id: UUID) -> tuple[UUID, ...]:
        strategy = self._topology.routing_strategy
        if strategy is RoutingStrategy.SEQUENTIAL:
            raise RoutingError("sequential routing does not allow broadcasts")
        if strategy is RoutingStrategy.HIERARCHICAL and sender_id != self._hub:
            raise RoutingError(
                f"hierarchical routing: only the orchestrator may broadcast, "
                f"not {self._label(sender_id)}"
            )
        return tuple(agent_id for agent_id in self._order if agent_id != sender_id)

    def _check_direct(self, sender_id: UUID, target_id: UUID) -> None:
        strategy = self._topology.routing_strategy
        match strategy:
            case RoutingStrategy.SEQUENTIAL:
                if self.successor(sender_id) != target_id:
                    raise RoutingError(
                        f"sequential routing: {self._label(sender_id)} may only message "
                        f"the next pipeline step, not {self._label(target_id)}"
                    )
            case RoutingStrategy.HIERARCHICAL:
                if self._hub not in (sender_id, target_id):
                    raise RoutingError(
                        f"hierarchical routing: workers may only message the orchestrator "
                        f"({self._label(sender_id)} -> {self._label(target_id)})"
                    )
            case RoutingStrategy.MESH:
                pass
            case _:
                assert_never(strategy)

    def _require_agent(self, agent_id: UUID) -> None:
        if agent_id not in self._index:
            raise AgentNotFoundError(agent_id)

    def _label(self, agent_id: UUID) -> str:
        return repr(self._names.get(agent_id, str(agent_id)))
