"""Shared fixtures: hermetic guards, sample topologies, a mock SSE gateway, collectors."""

import asyncio
import json
import logging
import os
import socket
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

import httpx
import pytest

from agent_swarm_tui.client import GatewayClient, GatewayClientSettings
from agent_swarm_tui.core import AgentReply, SwarmEvent
from agent_swarm_tui.schemas import (
    AgentRole,
    AgentState,
    RoutingStrategy,
    SwarmMessage,
    SwarmTopology,
)

E = TypeVar("E")

DONE = b"data: [DONE]\n\n"
RESET_AT = datetime(2030, 1, 1, tzinfo=UTC)


# --- Hermetic guards ------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail loudly on any real network connection attempt."""

    def refuse(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("network access is disabled in tests")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket.socket, "connect_ex", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.fixture(autouse=True)
def _clean_gateway_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The developer's GATEWAY_* variables must not leak into settings defaults."""
    for key in list(os.environ):
        if key.startswith("GATEWAY_"):
            monkeypatch.delenv(key)


@pytest.fixture
def restore_logging() -> Iterator[None]:
    """Undo ``logging.basicConfig(force=True)`` done by the CLI."""
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    for handler in root.handlers:
        if handler not in handlers:
            handler.close()
    root.handlers[:] = handlers
    root.setLevel(level)


# --- Domain fixtures ---------------------------------------------------------------------


def make_agent(name: str, role: AgentRole = AgentRole.EXECUTOR) -> AgentState:
    """An IDLE agent."""
    return AgentState(name=name, role=role)


def make_topology(strategy: RoutingStrategy, *agents: AgentState, name: str = "t") -> SwarmTopology:
    """A topology over ``agents``."""
    return SwarmTopology(name=name, agents=agents, routing_strategy=strategy)


@pytest.fixture
def sequential_topology() -> SwarmTopology:
    """p1 -> p2 -> p3."""
    return make_topology(
        RoutingStrategy.SEQUENTIAL, make_agent("p1"), make_agent("p2"), make_agent("p3")
    )


@pytest.fixture
def hierarchical_topology() -> SwarmTopology:
    """boss (orchestrator) over w1, w2; the orchestrator is deliberately not first."""
    return make_topology(
        RoutingStrategy.HIERARCHICAL,
        make_agent("w1"),
        make_agent("boss", AgentRole.ORCHESTRATOR),
        make_agent("w2"),
    )


@pytest.fixture
def mesh_topology() -> SwarmTopology:
    """Three peers."""
    return make_topology(RoutingStrategy.MESH, make_agent("m1"), make_agent("m2"), make_agent("m3"))


# --- Collectors and runners --------------------------------------------------------------


class EventCollector:
    """Orchestrator listener that records every event."""

    def __init__(self) -> None:
        self.events: list[SwarmEvent] = []

    def __call__(self, event: SwarmEvent) -> None:
        self.events.append(event)

    def of(self, kind: type[E]) -> list[E]:
        """Events of one type, in order."""
        return [event for event in self.events if isinstance(event, kind)]


@pytest.fixture
def events() -> EventCollector:
    """A fresh event collector."""
    return EventCollector()


type RunFn = Callable[[AgentState, SwarmMessage], Awaitable[AgentReply]]


class ScriptedRunner:
    """``AgentRunner`` whose behaviour is a coroutine function; records calls."""

    def __init__(self, fn: RunFn) -> None:
        self.fn = fn
        self.calls: list[tuple[str, str]] = []

    async def run(self, agent: AgentState, message: SwarmMessage) -> AgentReply:
        self.calls.append((agent.name, message.payload))
        return await self.fn(agent, message)


async def wait_for(condition: Callable[[], bool], within: float = 5.0) -> bool:
    """Poll ``condition`` on the event loop until true or ``within`` seconds pass."""
    deadline = time.monotonic() + within
    while not condition():
        if time.monotonic() > deadline:
            return False
        await asyncio.sleep(0.01)
    return True


# --- Mock SSE gateway --------------------------------------------------------------------


def sse(obj: object) -> bytes:
    """One ``data:`` frame with a JSON payload."""
    return b"data: " + json.dumps(obj).encode() + b"\n\n"


def summary(**overrides: Any) -> dict[str, Any]:
    """A valid ``summary`` frame payload."""
    return {
        "type": "summary",
        "model_used": "stub-model",
        "provider": "vllm",
        "tokens_used": 10,
        "cached_hit": False,
        "latency_ms": 5.0,
        **overrides,
    }


def delta(text: str) -> dict[str, str]:
    """A ``delta`` frame payload."""
    return {"type": "delta", "content": text}


def good_stream(*parts: str, **summary_overrides: Any) -> list[bytes]:
    """Deltas, a summary and ``[DONE]``."""
    return [*(sse(delta(p)) for p in parts), sse(summary(**summary_overrides)), DONE]


def rate_limit_headers(remaining: int, *, limit: int = 60) -> dict[str, str]:
    """The gateway's ``X-RateLimit-*`` headers."""
    return {
        "x-ratelimit-limit": str(limit),
        "x-ratelimit-remaining": str(remaining),
        "x-ratelimit-reset": str(int(RESET_AT.timestamp())),
    }


@dataclass
class Reply:
    """One scripted gateway response."""

    status: int = 200
    chunks: list[bytes] = field(default_factory=list)
    headers: dict[str, str] = field(default_factory=dict)
    json_body: Any = None
    ttft: float = 0.0
    delay: float = 0.0


def agent_name_in(prompt: str) -> str:
    """The agent name from the default prompt template (``You are <name>, ...``)."""
    return prompt.split("You are ", 1)[1].split(",", 1)[0] if "You are " in prompt else "?"


def echo_responder(request_body: dict[str, Any]) -> Reply:
    """Default behaviour: ``<agent> says done`` with 10 tokens."""
    who = agent_name_in(request_body["prompt"])
    return Reply(chunks=good_stream(f"{who} ", "says ", "done"))


class MockGateway:
    """In-process stand-in for ai-inference-gateway's ``POST /v1/inference``.

    ``responder`` maps each parsed request body to a ``Reply``. Requests are
    recorded; streams cancelled mid-flight are counted.
    """

    def __init__(self) -> None:
        self.responder: Callable[[dict[str, Any]], Reply] = echo_responder
        self.requests: list[httpx.Request] = []
        self.cancelled = 0

    @property
    def bodies(self) -> list[dict[str, Any]]:
        """Parsed JSON bodies of all requests so far."""
        return [json.loads(request.content) for request in self.requests]

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        reply = self.responder(json.loads(request.content))
        if reply.json_body is not None:
            return httpx.Response(reply.status, json=reply.json_body, headers=reply.headers)

        async def body() -> AsyncIterator[bytes]:
            try:
                if reply.ttft:
                    await asyncio.sleep(reply.ttft)
                for chunk in reply.chunks:
                    if reply.delay:
                        await asyncio.sleep(reply.delay)
                    yield chunk
            except asyncio.CancelledError:
                self.cancelled += 1
                raise

        headers = {"content-type": "text/event-stream", **reply.headers}
        return httpx.Response(reply.status, headers=headers, content=body())

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    def client(self, **settings: Any) -> GatewayClient:
        """A client wired to this mock (caller closes it)."""
        return GatewayClient(GatewayClientSettings(**settings), transport=self.transport)


@pytest.fixture
def gateway() -> MockGateway:
    """A fresh mock gateway."""
    return MockGateway()


@pytest.fixture
async def client(gateway: MockGateway) -> AsyncIterator[GatewayClient]:
    """A gateway client on the mock transport, closed after the test."""
    async with gateway.client() as gateway_client:
        yield gateway_client


@pytest.fixture
def future_reset() -> datetime:
    """A rate-limit reset time comfortably in the future."""
    return datetime.now(UTC) + timedelta(minutes=5)
