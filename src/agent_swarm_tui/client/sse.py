r"""Incremental, bounded Server-Sent Events decoder (WHATWG HTML spec, ``data`` only).

Lines end at ``\r\n``, ``\n`` or ``\r`` (and nothing else: raw U+2028 inside
JSON must not split a frame). ``data`` lines accumulate until a blank line
dispatches the event; comments and the ``event``/``id``/``retry`` fields are
ignored because the gateway does not use them. An event still incomplete at
end of stream is discarded, as the spec requires.
"""

import codecs
import re
from typing import Final

from agent_swarm_tui.client.errors import GatewayProtocolError

MAX_EVENT_CHARS: Final = 1_048_576

_LINE_BREAK: Final = re.compile(r"\r\n|\r|\n")
_BOM: Final = "﻿"


class SSEDecoder:
    """Turns arbitrary byte chunks into complete ``data`` payloads.

    Raises ``GatewayProtocolError`` for invalid UTF-8 and for any line or
    event larger than ``max_event_chars`` (protects memory against a
    misbehaving server).
    """

    def __init__(self, *, max_event_chars: int = MAX_EVENT_CHARS) -> None:
        """Create a decoder for one stream."""
        if max_event_chars < 1:
            raise ValueError(f"max_event_chars must be >= 1, got {max_event_chars}")
        self._max = max_event_chars
        self._utf8 = codecs.getincrementaldecoder("utf-8")(errors="strict")
        self._partial = ""
        self._data: list[str] = []
        self._size = 0
        self._started = False
        self._skip_lf = False

    def feed(self, chunk: bytes) -> list[str]:
        """Consume ``chunk``; return the payloads of every event it completed."""
        try:
            text = self._utf8.decode(chunk)
        except UnicodeDecodeError as exc:
            raise GatewayProtocolError("event stream is not valid UTF-8") from exc
        if not text:  # only part of a multi-byte character so far
            return []
        if not self._started:
            self._started = True
            text = text.removeprefix(_BOM)
        # A CR ends its line at once (no added latency); if the previous chunk
        # ended with CR, a leading LF is the second half of that CRLF.
        if self._skip_lf:
            text = text.removeprefix("\n")
        text = self._partial + text
        self._skip_lf = text.endswith("\r")
        *lines, rest = _LINE_BREAK.split(text)
        self._partial = rest
        if len(self._partial) > self._max:
            raise GatewayProtocolError(f"event stream line exceeds {self._max} characters")
        return [payload for line in lines if (payload := self._line(line)) is not None]

    def _line(self, line: str) -> str | None:
        if not line:
            if not self._data:
                return None
            payload = "\n".join(self._data)
            self._data.clear()
            self._size = 0
            return payload
        if line.startswith(":"):
            return None
        name, _, value = line.partition(":")
        if name != "data":
            return None
        value = value.removeprefix(" ")
        self._size += len(value) + 1
        if self._size > self._max:
            raise GatewayProtocolError(f"event exceeds {self._max} characters")
        self._data.append(value)
        return None
