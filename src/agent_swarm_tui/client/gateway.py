"""Async ``httpx`` client for ai-inference-gateway's streaming inference endpoint.

A stream is ``delta*`` then exactly one ``summary``, then ``data: [DONE]``; an
``error`` frame replaces the summary when the upstream fails mid-stream.
Failures before the stream starts arrive as HTTP statuses with an
``ErrorResponse`` body.

Usage::

    async with GatewayClient() as client:
        async with client.stream(InferenceRequest(prompt="hi")) as stream:
            async for event in stream:
                ...
"""

import asyncio
import email.utils
import json
import logging
import math
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from types import TracebackType
from typing import Final, Self

import httpx
from pydantic import ValidationError

from agent_swarm_tui import __version__
from agent_swarm_tui.client.errors import (
    GatewayConnectionError,
    GatewayHTTPError,
    GatewayProtocolError,
    GatewayRateLimitError,
    GatewayStreamError,
    GatewayTimeoutError,
)
from agent_swarm_tui.client.protocol import (
    DONE_SENTINEL,
    INFERENCE_PATH,
    ErrorResponse,
    InferenceRequest,
    InferenceResult,
    RateLimitInfo,
    StreamDelta,
    StreamErrorFrame,
    StreamEvent,
    StreamSummary,
    WireModel,
    utc_from_timestamp,
)
from agent_swarm_tui.client.settings import GatewayClientSettings
from agent_swarm_tui.client.sse import SSEDecoder

logger = logging.getLogger(__name__)

MAX_ERROR_BODY_BYTES: Final = 65_536
_FRAMES: Final[dict[str, type[WireModel]]] = {
    "delta": StreamDelta,
    "summary": StreamSummary,
    "error": StreamErrorFrame,
}


class InferenceStream:
    """One open inference stream. Iterate it exactly once.

    Yields ``StreamDelta`` events and finally the ``StreamSummary``, then
    stops at ``[DONE]``. Raises ``GatewayStreamError`` for an ``error`` frame
    and ``GatewayProtocolError`` for malformed or truncated streams.
    """

    def __init__(
        self, response: httpx.Response, *, rate_limit: RateLimitInfo | None, deadline: float
    ) -> None:
        """Wrap an already validated ``text/event-stream`` response."""
        self._response = response
        self._deadline = deadline
        self._consumed = False
        self.rate_limit = rate_limit
        self.summary: StreamSummary | None = None

    def __aiter__(self) -> AsyncIterator[StreamEvent]:
        """Iterate the stream's events (single use)."""
        if self._consumed:
            raise RuntimeError("an InferenceStream can only be iterated once")
        self._consumed = True
        return self._events()

    async def collect(self) -> InferenceResult:
        """Consume the whole stream and return its content and summary."""
        parts = [event.content async for event in self if isinstance(event, StreamDelta)]
        if self.summary is None:  # pragma: no cover - _events guarantees a summary
            raise GatewayProtocolError("stream ended without a summary")
        return InferenceResult(
            content="".join(parts), summary=self.summary, rate_limit=self.rate_limit
        )

    async def _events(self) -> AsyncIterator[StreamEvent]:
        async for data in self._payloads():
            if data == DONE_SENTINEL:
                if self.summary is None:
                    raise GatewayProtocolError("stream reached [DONE] without a summary")
                return
            if self.summary is not None:
                raise GatewayProtocolError("unexpected frame after the summary")
            frame = _parse_frame(data)
            if frame is None:
                continue
            if isinstance(frame, StreamErrorFrame):
                detail = frame.error
                raise GatewayStreamError(
                    f"gateway stream failed: {detail.message}",
                    code=detail.code,
                    retryable=detail.retryable,
                )
            if isinstance(frame, StreamSummary):
                self.summary = frame
                yield frame
            elif isinstance(frame, StreamDelta):
                yield frame
        raise GatewayProtocolError("stream closed before [DONE]")

    async def _payloads(self) -> AsyncIterator[str]:
        decoder = SSEDecoder()
        chunks = self._response.aiter_bytes()
        while True:
            with _translate_transport_errors():
                async with asyncio.timeout_at(self._deadline):
                    try:
                        chunk = await anext(chunks)
                    except StopAsyncIteration:
                        return
            for payload in decoder.feed(chunk):
                yield payload


class GatewayClient:
    """Owns one pooled ``httpx.AsyncClient``; close it with ``aclose`` or ``async with``.

    ``last_rate_limit`` holds the ``X-RateLimit-*`` state from the most recent
    response that carried those headers (the gateway omits them when rate
    limiting is disabled or failing open).
    """

    def __init__(
        self,
        settings: GatewayClientSettings | None = None,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Create the HTTP client (``transport`` is injectable for tests)."""
        self._settings = settings if settings is not None else GatewayClientSettings()
        s = self._settings
        headers = {
            "accept": "text/event-stream",
            "user-agent": f"agent-swarm-tui/{__version__}",
        }
        if s.auth_token is not None:
            headers["authorization"] = f"Bearer {s.auth_token.get_secret_value()}"
        self._http = httpx.AsyncClient(
            base_url=str(s.base_url),
            headers=headers,
            timeout=httpx.Timeout(
                connect=s.connect_timeout_s,
                read=s.read_timeout_s,
                write=s.write_timeout_s,
                pool=s.pool_timeout_s,
            ),
            limits=httpx.Limits(
                max_connections=s.max_connections, max_keepalive_connections=s.max_connections
            ),
            follow_redirects=False,
            trust_env=s.trust_env,
            transport=transport,
        )
        self.last_rate_limit: RateLimitInfo | None = None

    @property
    def settings(self) -> GatewayClientSettings:
        """The settings this client was built with."""
        return self._settings

    async def aclose(self) -> None:
        """Close the connection pool."""
        await self._http.aclose()

    async def __aenter__(self) -> Self:
        """Return the client."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the connection pool."""
        await self.aclose()

    @asynccontextmanager
    async def stream(self, request: InferenceRequest) -> AsyncIterator[InferenceStream]:
        """Open a streaming inference request; the response is closed on exit.

        Raises (on entry) ``GatewayRateLimitError`` for 429, ``GatewayHTTPError``
        for other non-2xx statuses, ``GatewayProtocolError`` for a non-SSE
        success, and ``GatewayConnectionError`` / ``GatewayTimeoutError`` for
        transport failures. The whole request, including iteration, is
        bounded by ``settings.stream_timeout_s``.
        """
        deadline = asyncio.get_running_loop().time() + self._settings.stream_timeout_s
        http_request = self._http.build_request("POST", INFERENCE_PATH, json=request.to_payload())
        with _translate_transport_errors():
            async with asyncio.timeout_at(deadline):
                response = await self._http.send(http_request, stream=True)
        try:
            rate_limit = _parse_rate_limit(response.headers)
            if rate_limit is not None:
                self.last_rate_limit = rate_limit
            await _raise_for_response(response, rate_limit, deadline)
            yield InferenceStream(response, rate_limit=rate_limit, deadline=deadline)
        finally:
            await response.aclose()

    async def infer(self, request: InferenceRequest) -> InferenceResult:
        """Run ``request`` to completion and return the collected result."""
        async with self.stream(request) as stream:
            return await stream.collect()


@contextmanager
def _translate_transport_errors() -> Iterator[None]:
    try:
        yield
    except TimeoutError as exc:
        raise GatewayTimeoutError("gateway request exceeded the stream deadline") from exc
    except httpx.TimeoutException as exc:
        raise GatewayTimeoutError(f"gateway timed out ({type(exc).__name__})") from exc
    except httpx.TransportError as exc:
        raise GatewayConnectionError(
            f"gateway connection failed ({type(exc).__name__}: {exc})"
        ) from exc


async def _raise_for_response(
    response: httpx.Response, rate_limit: RateLimitInfo | None, deadline: float
) -> None:
    if response.is_success:
        content_type = response.headers.get("content-type", "")
        if content_type.split(";", 1)[0].strip().lower() != "text/event-stream":
            raise GatewayProtocolError(
                f"expected text/event-stream, got {content_type or 'no content type'!r}"
            )
        return
    with _translate_transport_errors():
        async with asyncio.timeout_at(deadline):
            body = await _read_capped(response, MAX_ERROR_BODY_BYTES)
    try:
        detail = ErrorResponse.model_validate_json(body).error
    except ValidationError:
        code, message, retryable = None, response.reason_phrase or "error", False
    else:
        code, message, retryable = detail.code, detail.message, detail.retryable
    summary = f"gateway returned HTTP {response.status_code}: {message}"
    if response.status_code == httpx.codes.TOO_MANY_REQUESTS:
        retry_after = _parse_retry_after(response.headers.get("retry-after"))
        if rate_limit is not None and retry_after is not None:
            rate_limit = rate_limit.model_copy(update={"retry_after_s": retry_after})
        raise GatewayRateLimitError(
            summary, code=code, rate_limit=rate_limit, retry_after_s=retry_after
        )
    raise GatewayHTTPError(
        summary,
        status_code=response.status_code,
        code=code,
        retryable=retryable,
        rate_limit=rate_limit,
    )


async def _read_capped(response: httpx.Response, limit: int) -> bytes:
    body = bytearray()
    async for chunk in response.aiter_bytes():
        body += chunk
        if len(body) >= limit:
            break
    return bytes(body[:limit])


def _parse_frame(data: str) -> WireModel | None:
    try:
        obj = json.loads(data)
    except json.JSONDecodeError as exc:
        raise GatewayProtocolError("SSE frame is not valid JSON") from exc
    if not isinstance(obj, dict):
        raise GatewayProtocolError("SSE frame is not a JSON object")
    kind = obj.get("type")
    model = _FRAMES.get(kind) if isinstance(kind, str) else None
    if model is None:
        logger.debug("ignoring SSE frame of unknown type %r", kind)
        return None
    try:
        return model.model_validate(obj)
    except ValidationError as exc:
        raise GatewayProtocolError(f"invalid {kind!r} frame: {exc.error_count()} error(s)") from exc


def _parse_rate_limit(headers: httpx.Headers) -> RateLimitInfo | None:
    raw = (
        headers.get("x-ratelimit-limit"),
        headers.get("x-ratelimit-remaining"),
        headers.get("x-ratelimit-reset"),
    )
    if any(value is None for value in raw):
        return None
    try:
        limit, remaining, reset = (int(value) for value in raw if value is not None)
        return RateLimitInfo(
            limit=limit,
            remaining=remaining,
            reset_at=utc_from_timestamp(reset),
            retry_after_s=_parse_retry_after(headers.get("retry-after")),
        )
    except (ValueError, OverflowError, OSError):
        logger.debug("ignoring malformed X-RateLimit-* headers %r", raw)
        return None


def _parse_retry_after(value: str | None) -> float | None:
    """``Retry-After`` as delay-seconds or an HTTP-date (RFC 9110 section 10.2.3)."""
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            when = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = (when - datetime.now(UTC)).total_seconds()
    if not math.isfinite(seconds):
        return None
    return max(0.0, seconds)
