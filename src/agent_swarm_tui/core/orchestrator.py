"""Swarm lifecycle: one asyncio task per agent under a supervising ``TaskGroup``.

Each agent loop takes messages from its mailbox one at a time and drives the
agent through the state machine (``-> RUNNING -> IDLE``, or ``-> ERROR`` on
failure). Runner failures are contained per agent; only a bug in the loop
itself takes the ``TaskGroup`` (and so the swarm) down.
"""

import asyncio
import logging
import math
import time
from collections import deque
from collections.abc import Callable, Sequence
from enum import Enum, auto
from types import TracebackType
from typing import Final, Self
from uuid import UUID

from agent_swarm_tui.core.errors import AgentNotFoundError, RoutingError, SwarmError
from agent_swarm_tui.core.events import (
    MAX_ERROR_CHARS,
    AgentFailed,
    AgentUpdated,
    MessageRouted,
    SwarmEvent,
    TaskCompleted,
)
from agent_swarm_tui.core.ports import AgentReply, AgentRunner
from agent_swarm_tui.core.router import DEFAULT_MAILBOX_SIZE, MessageRouter
from agent_swarm_tui.core.state_machine import can_transition, record_usage, transition
from agent_swarm_tui.schemas import (
    MAX_TASK_CHARS,
    AgentState,
    AgentStatus,
    MessageType,
    SwarmMessage,
    SwarmMetrics,
    SwarmTopology,
)

logger = logging.getLogger(__name__)

DEFAULT_DRAIN_TIMEOUT: Final = 10.0
MAX_RETAINED_RESULTS: Final = 256

_ACTIVE: Final = frozenset({AgentStatus.RUNNING, AgentStatus.WAITING})
_NOT_STARTABLE: Final = frozenset({AgentStatus.RUNNING, AgentStatus.WAITING})

type SwarmListener = Callable[[SwarmEvent], None]


class _Phase(Enum):
    NEW = auto()
    RUNNING = auto()
    STOPPED = auto()


class SwarmOrchestrator:
    """Runs a ``SwarmTopology``: lifecycle, task dispatch, live state and metrics.

    Single-use: ``start_swarm`` once, ``stop_swarm`` once (further stops are
    no-ops). Also usable as ``async with SwarmOrchestrator(...) as swarm``.

    ``listener`` is called synchronously, on the event loop, for every
    ``SwarmEvent``; it must not block. Listener exceptions are logged and
    swallowed so a faulty observer cannot stall the swarm.
    """

    def __init__(
        self,
        topology: SwarmTopology,
        runner: AgentRunner,
        *,
        listener: SwarmListener | None = None,
        mailbox_size: int = DEFAULT_MAILBOX_SIZE,
        cost_per_million_tokens: float = 0.0,
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        """Prepare (but do not start) the swarm.

        Agents must start ``IDLE``, ``ERROR`` or ``TERMINATED``; ``clock`` returns
        seconds and is used only to measure per-message latency.
        """
        busy = [a.name for a in topology.agents if a.status in _NOT_STARTABLE]
        if busy:
            raise SwarmError(f"agents must not start running or waiting: {', '.join(busy)}")
        if not math.isfinite(cost_per_million_tokens) or cost_per_million_tokens < 0:
            raise ValueError(
                f"cost_per_million_tokens must be finite and >= 0, got {cost_per_million_tokens}"
            )
        self._topology = topology
        self._runner = runner
        self._listener = listener
        self._cost_per_token = cost_per_million_tokens / 1_000_000
        self._clock = clock
        self._router = MessageRouter(topology, mailbox_size=mailbox_size)
        self._states: dict[UUID, AgentState] = {agent.id: agent for agent in topology.agents}
        self._results: deque[TaskCompleted] = deque(maxlen=MAX_RETAINED_RESULTS)
        self._processed = 0
        self._in_flight = 0
        self._quiescent = asyncio.Event()
        self._quiescent.set()
        self._phase = _Phase.NEW
        self._supervisor: asyncio.Task[None] | None = None

    # --- Introspection -------------------------------------------------------

    @property
    def topology(self) -> SwarmTopology:
        """The topology as configured (initial agent states)."""
        return self._topology

    @property
    def is_running(self) -> bool:
        """Whether the swarm accepts tasks."""
        return (
            self._phase is _Phase.RUNNING
            and self._supervisor is not None
            and not self._supervisor.done()
        )

    @property
    def agents(self) -> tuple[AgentState, ...]:
        """Current state of every agent, in topology order."""
        return tuple(self._states.values())

    @property
    def results(self) -> tuple[TaskCompleted, ...]:
        """The most recent final outputs (up to ``MAX_RETAINED_RESULTS``), oldest first."""
        return tuple(self._results)

    @property
    def metrics(self) -> SwarmMetrics:
        """Live aggregate counters."""
        total_tokens = sum(state.total_tokens for state in self._states.values())
        return SwarmMetrics(
            active_agents=sum(state.status in _ACTIVE for state in self._states.values()),
            messages_processed=self._processed,
            total_tokens=total_tokens,
            estimated_cost_usd=total_tokens * self._cost_per_token,
        )

    def agent(self, agent_id: UUID) -> AgentState:
        """Return the current state of ``agent_id``."""
        try:
            return self._states[agent_id]
        except KeyError:
            raise AgentNotFoundError(agent_id) from None

    # --- Lifecycle -----------------------------------------------------------

    async def start_swarm(self) -> None:
        """Spawn one task per agent and start accepting tasks."""
        if self._phase is not _Phase.NEW:
            raise SwarmError("a swarm can only be started once")
        self._phase = _Phase.RUNNING
        self._supervisor = asyncio.create_task(
            self._supervise(), name=f"swarm:{self._topology.name}"
        )
        self._supervisor.add_done_callback(self._on_supervisor_done)
        await asyncio.sleep(0)  # let every agent loop reach its first ``get()``

    async def stop_swarm(
        self, *, drain: bool = True, drain_timeout: float | None = DEFAULT_DRAIN_TIMEOUT
    ) -> bool:
        """Stop the swarm and terminate every agent; return whether it drained cleanly.

        With ``drain`` the swarm first finishes every queued and in-flight
        message (including messages agents forward meanwhile) for up to
        ``drain_timeout`` seconds; new ``dispatch_task`` calls are rejected
        from the moment this is called. Anything left after the timeout, or
        everything when ``drain=False``, is cancelled.

        Raises ``SwarmError`` if an agent loop crashed while running.
        """
        supervisor = self._supervisor
        if self._phase is not _Phase.RUNNING or supervisor is None:
            self._phase = _Phase.STOPPED
            return True
        self._phase = _Phase.STOPPED
        drained = False
        try:
            if drain:
                drained = await self._await_quiescence(supervisor, drain_timeout)
                if not drained:
                    logger.warning(
                        "swarm %r did not drain in time; cancelling", self._topology.name
                    )
        finally:
            self._router.shutdown(immediate=not drained)
            if not drained:
                supervisor.cancel()
        await asyncio.wait((supervisor,))
        self._terminate_all()
        if not supervisor.cancelled() and (crash := supervisor.exception()) is not None:
            raise SwarmError(f"swarm {self._topology.name!r} crashed") from crash
        return drained

    async def __aenter__(self) -> Self:
        """Start the swarm."""
        await self.start_swarm()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Stop the swarm, draining only when the block exited cleanly."""
        await self.stop_swarm(drain=exc is None)

    # --- Work ----------------------------------------------------------------

    def dispatch_task(self, task: str, *, agent_id: UUID | None = None) -> SwarmMessage:
        """Send ``task`` to ``agent_id`` (default: the router's entry point).

        Returns the ``SYSTEM`` message that was enqueued. Raises ``SwarmError``
        when the swarm is not running, ``AgentNotFoundError`` for an unknown
        agent, ``RoutingError`` when its mailbox is full, and ``ValueError``
        for an empty or oversized task.
        """
        if not self.is_running:
            raise SwarmError("swarm is not running")
        message = SwarmMessage(
            sender_id=self._topology.swarm_id,
            target_id=self._router.entry_point if agent_id is None else agent_id,
            payload=task,
            message_type=MessageType.SYSTEM,
        )
        self._deliver((message,))
        return message

    async def _supervise(self) -> None:
        async with asyncio.TaskGroup() as group:
            for agent in self._topology.agents:
                group.create_task(self._agent_loop(agent.id), name=f"agent:{agent.name}")

    async def _agent_loop(self, agent_id: UUID) -> None:
        mailbox = self._router.mailbox(agent_id)
        while True:
            try:
                message = await mailbox.get()
            except asyncio.QueueShutDown:
                return
            try:
                await self._handle(agent_id, message)
            finally:
                mailbox.task_done()
                self._settle()

    async def _handle(self, agent_id: UUID, message: SwarmMessage) -> None:
        state = self._states[agent_id]
        if not can_transition(state.status, AgentStatus.RUNNING):
            logger.debug("agent %r is %s; dropping message", state.name, state.status.value)
            return
        running = self._set_state(
            transition(state, AgentStatus.RUNNING, task=_task_label(message.payload))
        )
        started = self._clock()
        try:
            reply = await self._runner.run(running, message)
        except Exception as exc:
            self._processed += 1
            logger.exception("agent %r failed while handling a message", running.name)
            self._fail(agent_id, exc)
            return
        self._processed += 1
        latency_ms = max(0.0, (self._clock() - started) * 1000.0)
        self._set_state(
            record_usage(self._states[agent_id], tokens=reply.tokens_used, latency_ms=latency_ms)
        )
        try:
            self._forward(agent_id, reply)
        except SwarmError as exc:
            logger.warning("agent %r could not forward its output: %s", running.name, exc)
            self._fail(agent_id, exc)
            return
        self._set_state(transition(self._states[agent_id], AgentStatus.IDLE))

    def _forward(self, sender_id: UUID, reply: AgentReply) -> None:
        if reply.output is None:
            return
        if reply.broadcast:
            self._deliver(
                (
                    SwarmMessage(
                        sender_id=sender_id,
                        payload=reply.output,
                        message_type=MessageType.BROADCAST,
                    ),
                )
            )
            return
        targets = (
            self._router.default_targets(sender_id)
            if reply.forward_to is None
            else reply.forward_to
        )
        if not targets:
            result = TaskCompleted(agent_id=sender_id, output=reply.output)
            self._results.append(result)
            self._emit(result)
            return
        if sender_id in targets:
            raise RoutingError("an agent cannot forward output to itself")
        self._deliver(
            tuple(
                SwarmMessage(
                    sender_id=sender_id,
                    target_id=target_id,
                    payload=reply.output,
                    message_type=MessageType.DIRECT,
                )
                for target_id in targets
            )
        )

    # --- Bookkeeping ---------------------------------------------------------

    def _deliver(self, messages: Sequence[SwarmMessage]) -> None:
        plan = self._router.route_batch(messages)
        delivered = sum(len(recipients) for recipients in plan)
        if delivered:
            self._in_flight += delivered
            self._quiescent.clear()
        for message, recipients in zip(messages, plan, strict=True):
            self._emit(MessageRouted(message=message, recipients=recipients))

    def _settle(self) -> None:
        self._in_flight -= 1
        if self._in_flight == 0:
            self._quiescent.set()

    async def _await_quiescence(
        self, supervisor: asyncio.Task[None], drain_timeout: float | None
    ) -> bool:
        if self._quiescent.is_set():
            return True
        waiter = asyncio.create_task(self._quiescent.wait())
        try:
            await asyncio.wait(
                (waiter, supervisor), timeout=drain_timeout, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            waiter.cancel()
        return self._quiescent.is_set()

    def _set_state(self, state: AgentState) -> AgentState:
        self._states[state.id] = state
        self._emit(AgentUpdated(agent=state))
        return state

    def _fail(self, agent_id: UUID, exc: Exception) -> None:
        self._set_state(transition(self._states[agent_id], AgentStatus.ERROR))
        detail = f"{type(exc).__name__}: {exc}"
        self._emit(AgentFailed(agent_id=agent_id, error=detail[:MAX_ERROR_CHARS]))

    def _terminate_all(self) -> None:
        for state in tuple(self._states.values()):
            if state.status is not AgentStatus.TERMINATED:
                self._set_state(transition(state, AgentStatus.TERMINATED))

    def _emit(self, event: SwarmEvent) -> None:
        if self._listener is None:
            return
        try:
            self._listener(event)
        except Exception:
            logger.exception("swarm listener failed on %s", event.kind)

    def _on_supervisor_done(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.error("swarm %r crashed", self._topology.name, exc_info=exc)


def _task_label(payload: str) -> str:
    if len(payload) <= MAX_TASK_CHARS:
        return payload
    return payload[: MAX_TASK_CHARS - 3] + "..."
