"""ai-inference-gateway client: settings, SSE streaming and the agent runner adapter."""

from agent_swarm_tui.client.errors import (
    GatewayClientError,
    GatewayConnectionError,
    GatewayHTTPError,
    GatewayProtocolError,
    GatewayRateLimitError,
    GatewayStreamError,
    GatewayTimeoutError,
)
from agent_swarm_tui.client.gateway import GatewayClient, InferenceStream
from agent_swarm_tui.client.protocol import (
    InferenceOptions,
    InferenceRequest,
    InferenceResult,
    RateLimitInfo,
    StreamDelta,
    StreamEvent,
    StreamSummary,
)
from agent_swarm_tui.client.runner import (
    DEFAULT_PERSONAS,
    DEFAULT_TEMPLATE,
    GatewayAgentRunner,
    PromptTemplate,
    ReplyFactory,
    RunnerOutput,
    delegating_reply,
    next_hop_reply,
)
from agent_swarm_tui.client.settings import GatewayClientSettings
from agent_swarm_tui.client.sse import SSEDecoder

__all__ = [
    "DEFAULT_PERSONAS",
    "DEFAULT_TEMPLATE",
    "GatewayAgentRunner",
    "GatewayClient",
    "GatewayClientError",
    "GatewayClientSettings",
    "GatewayConnectionError",
    "GatewayHTTPError",
    "GatewayProtocolError",
    "GatewayRateLimitError",
    "GatewayStreamError",
    "GatewayTimeoutError",
    "InferenceOptions",
    "InferenceRequest",
    "InferenceResult",
    "InferenceStream",
    "PromptTemplate",
    "RateLimitInfo",
    "ReplyFactory",
    "RunnerOutput",
    "SSEDecoder",
    "StreamDelta",
    "StreamEvent",
    "StreamSummary",
    "delegating_reply",
    "next_hop_reply",
]
