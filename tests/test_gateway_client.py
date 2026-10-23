"""SSE decoding, settings, wire contracts, HTTP errors and rate-limit propagation."""

import asyncio
import json
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import Any

import httpx
import pytest
from pydantic import HttpUrl, SecretStr, ValidationError

from agent_swarm_tui.client import (
    GatewayClient,
    GatewayClientSettings,
    GatewayConnectionError,
    GatewayHTTPError,
    GatewayProtocolError,
    GatewayRateLimitError,
    GatewayStreamError,
    GatewayTimeoutError,
    InferenceOptions,
    InferenceRequest,
    SSEDecoder,
    StreamDelta,
    StreamSummary,
)
from agent_swarm_tui.client.gateway import _parse_rate_limit, _parse_retry_after
from agent_swarm_tui.client.protocol import utc_from_timestamp
from agent_swarm_tui.client.settings import _is_loopback
from agent_swarm_tui.core import SwarmError
from tests.conftest import (
    DONE,
    RESET_AT,
    MockGateway,
    Reply,
    delta,
    good_stream,
    rate_limit_headers,
    sse,
    summary,
)

REQ = InferenceRequest(prompt="hi", temperature=0.2)
LS_FRAME = 'data: {"c":"a' + chr(0x2028) + 'b"}\n\n'  # raw LINE SEPARATOR inside JSON


class TestSSEDecoder:
    @pytest.mark.parametrize(
        ("chunks", "expected"),
        [
            ([b"data: a\n\n"], ["a"]),
            ([b"data: a\r\n\r\n"], ["a"]),
            ([b"data: x\r\rdata: y\r\r"], ["x", "y"]),  # bare CR framing
            ([b"data: [DONE]\r\r"], ["[DONE]"]),  # CR-only end needs no next chunk
            ([b"data: a\r", b"\n\r\n"], ["a"]),  # CRLF split across chunks
            ([b"data: a\r", b"\n", b"\r\n"], ["a"]),
            ([b"data: a\r", b"", b"\n\n"], ["a"]),  # empty chunk keeps the pending CR state
            ([b"data: 1\r\n\ndata: 2\n\rdata: 3\r\r\n"], ["1", "2", "3"]),  # mixed endings
            ([b": keepalive\nevent: x\nid: 7\nretry: 5\ndata: l1\ndata:l2\n\n"], ["l1\nl2"]),
            ([b"data\n\n"], [""]),  # no colon: whole line is the field name, value ""
            ([b"\n\n\n"], []),  # blank lines without data dispatch nothing
            ([b"data:\n\n"], [""]),  # empty data line still dispatches
            ([(chr(0xFEFF) + "data: z\n\n").encode()], ["z"]),  # BOM stripped
            ([LS_FRAME.encode()], [LS_FRAME[6:-2]]),  # U+2028 is not a line ending
            ([b"data: incomplete"], []),  # unfinished event at EOF is discarded
        ],
    )
    def test_framing(self, chunks: list[bytes], expected: list[str]) -> None:
        decoder = SSEDecoder()
        assert [payload for chunk in chunks for payload in decoder.feed(chunk)] == expected

    def test_multibyte_character_split_across_chunks(self) -> None:
        raw = "data: \U0001f600\n\n".encode()
        decoder = SSEDecoder()
        assert decoder.feed(raw[:7]) == []
        assert decoder.feed(raw[7:]) == ["\U0001f600"]

    def test_byte_at_a_time(self) -> None:
        decoder = SSEDecoder()
        out = [p for byte in b"".join(good_stream("a", "b")) for p in decoder.feed(bytes([byte]))]
        assert [json.loads(p)["type"] if p != "[DONE]" else p for p in out] == [
            "delta",
            "delta",
            "summary",
            "[DONE]",
        ]

    @pytest.mark.parametrize(
        ("chunk", "error"),
        [
            (b"data: \xff\n\n", "not valid UTF-8"),
            (b"data: " + b"x" * 64, "line exceeds"),
            (b"data: 12345\n" * 5, "event exceeds"),
        ],
    )
    def test_protocol_violations(self, chunk: bytes, error: str) -> None:
        with pytest.raises(GatewayProtocolError, match=error):
            SSEDecoder(max_event_chars=16).feed(chunk)

    def test_limit_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="max_event_chars"):
            SSEDecoder(max_event_chars=0)


class TestSettings:
    def test_defaults(self) -> None:
        settings = GatewayClientSettings()
        assert str(settings.base_url) == "http://127.0.0.1:8000/"
        assert settings.auth_token is None
        assert settings.trust_env is False

    def test_reads_environment_and_ignores_foreign_gateway_vars(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GATEWAY_BASE_URL", "http://localhost:9000/prefix")
        monkeypatch.setenv("GATEWAY_READ_TIMEOUT_S", "7")
        monkeypatch.setenv("GATEWAY_REDIS_URL", "redis://x")  # the gateway server's own setting
        settings = GatewayClientSettings()
        assert (str(settings.base_url), settings.read_timeout_s) == (
            "http://localhost:9000/prefix",
            7.0,
        )

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"base_url": "not a url"},
            {"base_url": "http://user:pw@host:1"},
            {"base_url": "http://host:1/?q=1"},
            {"base_url": "http://host:1/#frag"},
            {"base_url": "http://gw.example.com", "auth_token": SecretStr("t")},
            {"read_timeout_s": 0},
            {"max_connections": 0},
            {"unknown_option": 1},
        ],
    )
    def test_rejects(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(ValidationError):
            GatewayClientSettings(**kwargs)

    @pytest.mark.parametrize(
        "url",
        ["https://gw.example.com", "http://127.0.0.2:1", "http://[::1]:1", "http://localhost:1"],
    )
    def test_token_allowed_over_https_or_loopback(self, url: str) -> None:
        settings = GatewayClientSettings(base_url=HttpUrl(url), auth_token=SecretStr("t0ps3cret"))
        assert "t0ps3cret" not in repr(settings)

    def test_loopback_helper(self) -> None:
        assert _is_loopback("127.0.0.1")
        assert not _is_loopback(None)
        assert not _is_loopback("example.com")
        assert not _is_loopback("10.0.0.1")


class TestProtocol:
    def test_payload_always_streams_and_omits_unset_options(self) -> None:
        assert REQ.to_payload() == {"prompt": "hi", "temperature": 0.2, "stream": True}

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"prompt": "   "},
            {"prompt": ""},
            {"prompt": "x", "temperature": 2.5},
            {"prompt": "x", "model": "bad model"},
        ],
    )
    def test_request_validation(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(ValidationError):
            InferenceRequest(**kwargs)

    def test_options_bounds(self) -> None:
        with pytest.raises(ValidationError):
            InferenceOptions(max_tokens=0)

    def test_timestamp_helper(self) -> None:
        assert utc_from_timestamp(int(RESET_AT.timestamp())) == RESET_AT


class TestHeaders:
    def test_rate_limit_headers(self) -> None:
        info = _parse_rate_limit(httpx.Headers(rate_limit_headers(41)))
        assert info is not None
        assert (info.limit, info.remaining, info.reset_at, info.retry_after_s) == (
            60,
            41,
            RESET_AT,
            None,
        )

    @pytest.mark.parametrize(
        "headers",
        [
            {},
            {"x-ratelimit-limit": "60", "x-ratelimit-remaining": "1"},  # incomplete
            {**rate_limit_headers(1), "x-ratelimit-remaining": "lots"},  # not an int
            {**rate_limit_headers(1), "x-ratelimit-remaining": "-1"},  # out of range
            {**rate_limit_headers(1), "x-ratelimit-reset": "99999999999999999999"},  # overflow
        ],
    )
    def test_missing_or_malformed_rate_limit_headers_are_ignored(
        self, headers: dict[str, str]
    ) -> None:
        assert _parse_rate_limit(httpx.Headers(headers)) is None

    def test_retry_after(self) -> None:
        future = datetime.now(UTC) + timedelta(seconds=30)
        assert _parse_retry_after(None) is None
        assert _parse_retry_after("7") == 7.0
        assert _parse_retry_after("-3") == 0.0
        assert _parse_retry_after("nan") is None
        assert _parse_retry_after("soon") is None
        assert _parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0  # past date
        assert 25 < (_parse_retry_after(format_datetime(future, usegmt=True)) or 0) <= 30
        naive = format_datetime(future.replace(tzinfo=None))  # "-0000": no zone information
        assert 25 < (_parse_retry_after(naive) or 0) <= 30


class TestStreaming:
    async def test_events_rate_limit_and_request(self, gateway: MockGateway) -> None:
        gateway.responder = lambda body: Reply(
            chunks=good_stream("Hel", "lo"), headers=rate_limit_headers(41)
        )
        async with gateway.client(auth_token=SecretStr("tok")) as client:
            assert client.settings.auth_token is not None
            async with client.stream(REQ) as stream:
                events = [event async for event in stream]
                with pytest.raises(RuntimeError, match="only be iterated once"):
                    stream.__aiter__()
            assert [type(e) for e in events] == [StreamDelta, StreamDelta, StreamSummary]
            assert stream.rate_limit is not None
            assert stream.rate_limit.remaining == 41
            assert stream.summary is not None
            assert client.last_rate_limit == stream.rate_limit
        request = gateway.requests[0]
        assert (request.method, request.url.path) == ("POST", "/v1/inference")
        assert json.loads(request.content) == {"prompt": "hi", "temperature": 0.2, "stream": True}
        assert request.headers["authorization"] == "Bearer tok"
        assert request.headers["accept"] == "text/event-stream"
        assert request.headers["user-agent"].startswith("agent-swarm-tui/")

    async def test_infer_collects(self, client: GatewayClient) -> None:
        result = await client.infer(REQ)
        assert result.content == "? says done"
        assert result.summary.tokens_used == 10
        assert result.rate_limit is None
        assert client.last_rate_limit is None  # gateway without rate limiting

    async def test_base_url_path_prefix_is_kept(self, gateway: MockGateway) -> None:
        async with gateway.client(base_url="http://127.0.0.1:1/gw") as client:
            await client.infer(REQ)
        assert gateway.requests[0].url.path == "/gw/v1/inference"

    async def test_tolerant_reader(self, gateway: MockGateway, client: GatewayClient) -> None:
        gateway.responder = lambda body: Reply(
            chunks=[
                sse({"type": "heartbeat"}),
                sse({**delta("a"), "new": 1}),
                sse(summary(future=True)),
                DONE,
            ]
        )
        assert (await client.infer(REQ)).content == "a"

    @pytest.mark.parametrize(
        ("chunks", "error", "match"),
        [
            (
                [
                    sse(delta("a")),
                    sse(
                        {
                            "type": "error",
                            "error": {
                                "code": "upstream_timeout",
                                "message": "slow",
                                "retryable": True,
                            },
                        }
                    ),
                    DONE,
                ],
                GatewayStreamError,
                "slow",
            ),
            ([sse(delta("a")), sse(summary())], GatewayProtocolError, "before \\[DONE\\]"),
            ([sse(delta("a")), DONE], GatewayProtocolError, "without a summary"),
            ([sse(summary()), sse(delta("late")), DONE], GatewayProtocolError, "after the summary"),
            ([b"data: {nope\n\n"], GatewayProtocolError, "not valid JSON"),
            ([b"data: [1, 2]\n\n"], GatewayProtocolError, "not a JSON object"),
            ([sse(summary(tokens_used=-1)), DONE], GatewayProtocolError, "invalid 'summary'"),
            ([sse(summary(tokens_used="10")), DONE], GatewayProtocolError, "invalid 'summary'"),
        ],
    )
    async def test_stream_failures(
        self,
        gateway: MockGateway,
        client: GatewayClient,
        chunks: list[bytes],
        error: type[Exception],
        match: str,
    ) -> None:
        gateway.responder = lambda body: Reply(chunks=chunks)
        with pytest.raises(error, match=match) as caught:
            await client.infer(REQ)
        if isinstance(caught.value, GatewayStreamError):
            assert (caught.value.code, caught.value.retryable) == ("upstream_timeout", True)
        assert isinstance(caught.value, SwarmError)

    async def test_early_exit_closes_the_response(
        self, gateway: MockGateway, client: GatewayClient
    ) -> None:
        closed: list[bool] = []

        class Tracked(httpx.AsyncByteStream):
            async def __aiter__(self) -> AsyncIterator[bytes]:
                for chunk in good_stream("a", "b"):
                    yield chunk

            async def aclose(self) -> None:
                closed.append(True)

        gateway.handler = lambda request: httpx.Response(  # type: ignore[method-assign]
            200, headers={"content-type": "text/event-stream"}, stream=Tracked()
        )
        async with GatewayClient(transport=gateway.transport) as own, own.stream(REQ) as stream:
            async for _ in stream:
                break
        assert closed == [True]


class TestHTTPErrors:
    async def test_429(self, gateway: MockGateway, client: GatewayClient) -> None:
        body = {
            "error": {"code": "rate_limited", "message": "Rate limit exceeded.", "retryable": True}
        }
        gateway.responder = lambda b: Reply(
            status=429, json_body=body, headers={**rate_limit_headers(0), "retry-after": "7"}
        )
        with pytest.raises(GatewayRateLimitError) as caught:
            await client.infer(REQ)
        error = caught.value
        assert isinstance(error, GatewayHTTPError)
        assert (error.status_code, error.code, error.retryable, error.retry_after_s) == (
            429,
            "rate_limited",
            True,
            7.0,
        )
        assert error.rate_limit is not None
        assert (error.rate_limit.remaining, error.rate_limit.retry_after_s) == (0, 7.0)
        assert client.last_rate_limit is not None
        assert client.last_rate_limit.remaining == 0

    async def test_429_without_headers(self, gateway: MockGateway, client: GatewayClient) -> None:
        gateway.responder = lambda b: Reply(
            status=429, json_body={"error": {"code": "rate_limited", "message": "x"}}
        )
        with pytest.raises(GatewayRateLimitError) as caught:
            await client.infer(REQ)
        assert (caught.value.retry_after_s, caught.value.rate_limit) == (None, None)

    @pytest.mark.parametrize(
        ("status", "body", "code", "retryable"),
        [
            (503, {"error": {"code": "no_route", "message": "No upstream."}}, "no_route", False),
            (
                504,
                {"error": {"code": "upstream_timeout", "message": "t", "retryable": True}},
                "upstream_timeout",
                True,
            ),
            (502, "<html>bad gateway</html>", None, False),
            (307, "", None, False),  # redirects are not followed
        ],
    )
    async def test_non_success_statuses(
        self,
        *,
        gateway: MockGateway,
        client: GatewayClient,
        status: int,
        body: object,
        code: str | None,
        retryable: bool,
    ) -> None:
        def respond(request: httpx.Request) -> httpx.Response:
            gateway.requests.append(request)
            if isinstance(body, str):
                return httpx.Response(status, text=body, headers={"location": "/elsewhere"})
            return httpx.Response(status, json=body)

        gateway.handler = respond  # type: ignore[method-assign]
        async with GatewayClient(transport=gateway.transport) as own:
            with pytest.raises(GatewayHTTPError) as caught:
                await own.infer(REQ)
        assert not isinstance(caught.value, GatewayRateLimitError)
        assert (caught.value.status_code, caught.value.code, caught.value.retryable) == (
            status,
            code,
            retryable,
        )
        assert len(gateway.requests) == 1

    async def test_huge_error_body_is_capped(self, gateway: MockGateway) -> None:
        gateway.handler = lambda request: httpx.Response(500, content=b"x" * 5_000_000)  # type: ignore[method-assign]
        async with GatewayClient(transport=gateway.transport) as own:
            with pytest.raises(GatewayHTTPError) as caught:
                await own.infer(REQ)
        assert caught.value.status_code == 500

    async def test_success_must_be_an_event_stream(
        self, gateway: MockGateway, client: GatewayClient
    ) -> None:
        gateway.responder = lambda b: Reply(json_body={"content": "json, not sse"})
        with pytest.raises(GatewayProtocolError, match="expected text/event-stream"):
            await client.infer(REQ)


class TestTransportErrors:
    async def test_suite_is_hermetic(self) -> None:
        """A real client (no mock transport) is stopped by the autouse network guard."""
        blocked = pytest.RaisesExc(RuntimeError, match="network access is disabled")
        async with GatewayClient() as real:
            # anyio's happy-eyeballs connect re-raises the guard's error inside a group.
            with pytest.RaisesGroup(blocked, flatten_subgroups=True):
                await real.infer(REQ)

    @pytest.mark.parametrize(
        ("exc", "expected"),
        [
            (httpx.ConnectError("refused"), GatewayConnectionError),
            (httpx.ReadTimeout("slow"), GatewayTimeoutError),
            (httpx.RemoteProtocolError("garbage"), GatewayConnectionError),
        ],
    )
    async def test_mapped_to_typed_errors(self, exc: Exception, expected: type[Exception]) -> None:
        def fail(request: httpx.Request) -> httpx.Response:
            raise exc

        async with GatewayClient(transport=httpx.MockTransport(fail)) as client:
            with pytest.raises(expected) as caught:
                await client.infer(REQ)
        assert getattr(caught.value, "retryable", None) is True
        assert caught.value.__cause__ is exc

    async def test_connection_drop_mid_stream(self, gateway: MockGateway) -> None:
        class Broken(httpx.AsyncByteStream):
            async def __aiter__(self) -> AsyncIterator[bytes]:
                yield sse(delta("a"))
                raise httpx.RemoteProtocolError("peer closed connection")

        gateway.handler = lambda request: httpx.Response(  # type: ignore[method-assign]
            200, headers={"content-type": "text/event-stream"}, stream=Broken()
        )
        async with GatewayClient(transport=gateway.transport) as client:
            with pytest.raises(GatewayConnectionError, match="RemoteProtocolError"):
                await client.infer(REQ)

    async def test_overall_deadline_is_enforced_mid_stream(self, gateway: MockGateway) -> None:
        gateway.responder = lambda body: Reply(chunks=good_stream("a", "b"), delay=0.2)
        async with gateway.client(stream_timeout_s=0.3) as client:
            started = time.monotonic()
            with pytest.raises(GatewayTimeoutError, match="deadline"):
                await client.infer(REQ)
        assert time.monotonic() - started < 2

    async def test_deadline_while_connecting(self) -> None:
        async def hang(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(10)
            raise AssertionError("unreachable")  # pragma: no cover

        transport = httpx.MockTransport(hang)
        async with GatewayClient(
            GatewayClientSettings(stream_timeout_s=0.05), transport=transport
        ) as client:
            with pytest.raises(GatewayTimeoutError):
                await client.infer(REQ)
