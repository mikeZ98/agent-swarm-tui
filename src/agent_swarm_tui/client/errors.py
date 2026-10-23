"""Typed failures of the ai-inference-gateway client.

Every transport, HTTP and protocol failure surfaces as one of these, so the
orchestrator (and the TUI) never handle ``httpx`` exception types.
"""

from agent_swarm_tui.client.protocol import RateLimitInfo
from agent_swarm_tui.core.errors import SwarmError


class GatewayClientError(SwarmError):
    """Base class for all gateway client errors.

    ``code`` is the gateway's machine-readable error code, when it sent one.
    """

    def __init__(self, message: str, *, code: str | None = None, retryable: bool = False) -> None:
        """Attach the gateway error code and retry hint."""
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class GatewayConnectionError(GatewayClientError):
    """The gateway could not be reached, or the connection broke mid-stream."""

    def __init__(self, message: str) -> None:
        """Transport failures are always worth retrying."""
        super().__init__(message, retryable=True)


class GatewayTimeoutError(GatewayConnectionError):
    """A connect/read/write/pool timeout or the overall stream deadline expired."""


class GatewayProtocolError(GatewayClientError):
    """The gateway's response violated the SSE / JSON contract."""


class GatewayHTTPError(GatewayClientError):
    """The gateway answered with a non-success HTTP status."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        code: str | None = None,
        retryable: bool = False,
        rate_limit: RateLimitInfo | None = None,
    ) -> None:
        """Attach the HTTP status and any rate-limit headers to the error."""
        super().__init__(message, code=code, retryable=retryable)
        self.status_code = status_code
        self.rate_limit = rate_limit


class GatewayRateLimitError(GatewayHTTPError):
    """HTTP 429: the gateway's token bucket for this client is exhausted."""

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        rate_limit: RateLimitInfo | None = None,
        retry_after_s: float | None = None,
    ) -> None:
        """Attach the server's ``Retry-After`` hint (seconds) to the error."""
        super().__init__(message, status_code=429, code=code, retryable=True, rate_limit=rate_limit)
        self.retry_after_s = retry_after_s


class GatewayStreamError(GatewayClientError):
    """The gateway reported an ``error`` frame after the stream had started."""
