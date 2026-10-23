"""``AgentRunner`` adapter: one agent message -> one streamed gateway inference.

While the request is in flight with no tokens yet, the agent is ``WAITING``;
the first streamed token flips it back to ``RUNNING`` (via the status hook the
orchestrator binds automatically).
"""

import logging
from collections.abc import Callable, Mapping
from string import Template
from types import MappingProxyType
from typing import Final
from uuid import UUID

from pydantic import Field, FiniteFloat

from agent_swarm_tui.client.gateway import GatewayClient
from agent_swarm_tui.client.protocol import (
    InferenceOptions,
    InferenceRequest,
    RateLimitInfo,
    StreamDelta,
)
from agent_swarm_tui.core import AgentReply, StatusHook, SwarmError
from agent_swarm_tui.schemas import (
    MAX_PAYLOAD_CHARS,
    AgentRole,
    AgentState,
    AgentStatus,
    MessageType,
    StrictModel,
    SwarmMessage,
)

logger = logging.getLogger(__name__)

MAX_TEMPLATE_CHARS: Final = 8192
TEMPLATE_FIELDS: Final = frozenset({"name", "role", "persona", "task", "payload", "message_type"})
TRUNCATION_MARKER: Final = "\n[output truncated]"

DEFAULT_TEMPLATE: Final = """\
You are $name, the $role agent in a multi-agent swarm.
$persona

Incoming $message_type message:
$payload"""

DEFAULT_PERSONAS: Final[Mapping[AgentRole, str]] = MappingProxyType(
    {
        AgentRole.ORCHESTRATOR: (
            "Coordinate the swarm: split the task into clear, independent sub-tasks "
            "and synthesize the workers' results into one answer."
        ),
        AgentRole.PLANNER: "Turn goals into a concrete, ordered and verifiable plan.",
        AgentRole.RESEARCHER: (
            "Gather and summarize the facts relevant to the input; state uncertainty explicitly."
        ),
        AgentRole.EXECUTOR: "Carry out the assigned step and report the concrete result.",
        AgentRole.REVIEWER: (
            "Critically review the input for errors, gaps and risks, and propose fixes."
        ),
    }
)


class RunnerOutput(StrictModel):
    """Outcome of one gateway call made on behalf of an agent."""

    agent_id: UUID
    content: str
    tokens_used: int = Field(ge=0)
    latency_ms: FiniteFloat = Field(ge=0.0, description="Gateway-reported latency.")
    model_used: str
    provider: str
    cached_hit: bool
    rate_limit: RateLimitInfo | None = None


type ReplyFactory = Callable[[AgentState, SwarmMessage, RunnerOutput], AgentReply]


class PromptTemplate:
    """A ``string.Template`` restricted to ``TEMPLATE_FIELDS``.

    ``$payload`` is mandatory. Substitution is single-pass, so ``$`` sequences
    inside the payload are never expanded.
    """

    def __init__(self, source: str = DEFAULT_TEMPLATE) -> None:
        """Validate ``source`` eagerly so a bad template fails at startup."""
        if len(source) > MAX_TEMPLATE_CHARS:
            raise ValueError(f"template exceeds {MAX_TEMPLATE_CHARS} characters")
        template = Template(source)
        if not template.is_valid():
            raise ValueError("template contains an invalid '$' placeholder (use '$$' for '$')")
        identifiers = set(template.get_identifiers())
        unknown = identifiers - TEMPLATE_FIELDS
        if unknown:
            raise ValueError(f"unknown template fields: {', '.join(sorted(unknown))}")
        if "payload" not in identifiers:
            raise ValueError("template must include $payload")
        self._template = template

    def render(self, agent: AgentState, message: SwarmMessage, *, persona: str) -> str:
        """Render the prompt for ``agent`` handling ``message``."""
        return self._template.substitute(
            name=agent.name,
            role=agent.role.value,
            persona=persona,
            task=agent.current_task or "",
            payload=message.payload,
            message_type=message.message_type.value,
        )


def next_hop_reply(agent: AgentState, message: SwarmMessage, output: RunnerOutput) -> AgentReply:
    """Forward the output along the routing strategy's default next hop."""
    del agent, message
    return AgentReply(tokens_used=output.tokens_used, output=_clip(output.content))


def delegating_reply(agent: AgentState, message: SwarmMessage, output: RunnerOutput) -> AgentReply:
    """For ``HIERARCHICAL`` swarms: the orchestrator broadcasts its plan for new tasks.

    Worker reports (and everything else) take the default next hop, so worker
    output flows back to the orchestrator and its synthesis is the result.
    """
    content = _clip(output.content)
    is_new_task = message.message_type is MessageType.SYSTEM
    if agent.role is AgentRole.ORCHESTRATOR and is_new_task and content is not None:
        return AgentReply(tokens_used=output.tokens_used, output=content, broadcast=True)
    return AgentReply(tokens_used=output.tokens_used, output=content)


class GatewayAgentRunner:
    """Runs each agent message as one streamed inference on ai-inference-gateway.

    ``on_delta`` receives ``(agent_id, text)`` for every streamed fragment and
    ``on_output`` the final ``RunnerOutput``; both run on the event loop and
    must not block (exceptions are logged and swallowed). The client is
    borrowed, not owned: close it yourself.
    """

    def __init__(
        self,
        client: GatewayClient,
        *,
        template: PromptTemplate | None = None,
        personas: Mapping[AgentRole, str] | None = None,
        options: InferenceOptions | None = None,
        reply_factory: ReplyFactory = next_hop_reply,
        on_delta: Callable[[UUID, str], None] | None = None,
        on_output: Callable[[RunnerOutput], None] | None = None,
    ) -> None:
        """Configure prompting, generation options and reply routing."""
        self._client = client
        self._template = template if template is not None else PromptTemplate()
        self._personas = {**DEFAULT_PERSONAS, **(personas or {})}
        self._options = options if options is not None else InferenceOptions()
        self._reply_factory = reply_factory
        self._on_delta = on_delta
        self._on_output = on_output
        self._status_hook: StatusHook | None = None

    def bind_status_hook(self, hook: StatusHook) -> None:
        """Receive the orchestrator's ``signal_status`` (``StatusAwareRunner``)."""
        self._status_hook = hook

    def render_prompt(self, agent: AgentState, message: SwarmMessage) -> str:
        """Build the prompt sent to the gateway for ``agent`` handling ``message``."""
        return self._template.render(agent, message, persona=self._personas[agent.role])

    async def run(self, agent: AgentState, message: SwarmMessage) -> AgentReply:
        """``AgentRunner``: call the gateway, then shape the reply."""
        output = await self.complete(agent, message)
        return self._reply_factory(agent, message, output)

    async def complete(self, agent: AgentState, message: SwarmMessage) -> RunnerOutput:
        """Stream one inference for ``agent`` and return its outcome.

        Raises ``GatewayClientError`` subclasses for gateway failures and
        ``ValueError`` if the rendered prompt violates the request contract.
        """
        request = InferenceRequest(
            prompt=self.render_prompt(agent, message), **self._options.model_dump()
        )
        parts: list[str] = []
        self._signal(agent.id, AgentStatus.WAITING)
        waiting = True
        try:
            async with self._client.stream(request) as stream:
                async for event in stream:
                    if not isinstance(event, StreamDelta):
                        continue
                    if waiting:
                        self._signal(agent.id, AgentStatus.RUNNING)
                        waiting = False
                    parts.append(event.content)
                    self._notify_delta(agent.id, event.content)
                summary, rate_limit = stream.summary, stream.rate_limit
        finally:
            if waiting:
                self._signal(agent.id, AgentStatus.RUNNING)
        if summary is None:  # pragma: no cover - InferenceStream guarantees a summary
            raise SwarmError("gateway stream ended without a summary")
        output = RunnerOutput(
            agent_id=agent.id,
            content="".join(parts),
            tokens_used=summary.tokens_used,
            latency_ms=summary.latency_ms,
            model_used=summary.model_used,
            provider=summary.provider,
            cached_hit=summary.cached_hit,
            rate_limit=rate_limit,
        )
        logger.debug(
            "agent %r: %d tokens via %s/%s in %.0f ms (cached=%s)",
            agent.name,
            output.tokens_used,
            output.provider,
            output.model_used,
            output.latency_ms,
            output.cached_hit,
        )
        self._notify_output(output)
        return output

    def _signal(self, agent_id: UUID, status: AgentStatus) -> None:
        if self._status_hook is None:
            return
        try:
            self._status_hook(agent_id, status)
        except SwarmError as exc:
            logger.debug("status hook rejected %s for agent %s: %s", status.value, agent_id, exc)

    def _notify_delta(self, agent_id: UUID, text: str) -> None:
        if self._on_delta is None:
            return
        try:
            self._on_delta(agent_id, text)
        except Exception:
            logger.exception("on_delta callback failed")

    def _notify_output(self, output: RunnerOutput) -> None:
        if self._on_output is None:
            return
        try:
            self._on_output(output)
        except Exception:
            logger.exception("on_output callback failed")


def _clip(content: str) -> str | None:
    if not content.strip():
        return None
    if len(content) <= MAX_PAYLOAD_CHARS:
        return content
    logger.warning("agent output of %d chars truncated to %d", len(content), MAX_PAYLOAD_CHARS)
    return content[: MAX_PAYLOAD_CHARS - len(TRUNCATION_MARKER)] + TRUNCATION_MARKER
