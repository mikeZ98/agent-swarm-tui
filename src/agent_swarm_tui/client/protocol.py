"""Wire contracts of ai-inference-gateway's ``POST /v1/inference`` (Repo 01).

Outbound models are fully strict (``extra="forbid"``). Inbound models use
``WireModel``: still frozen and non-coercing, but ``extra="ignore"`` so a new
optional field on the gateway does not break deployed clients (tolerant
reader). Contract violations in known fields are still rejected.
"""

from datetime import UTC, datetime
from typing import Any, Final, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, FiniteFloat, field_validator

from agent_swarm_tui.schemas import StrictModel

INFERENCE_PATH: Final = "/v1/inference"
DONE_SENTINEL: Final = "[DONE]"
MAX_PROMPT_CHARS: Final = 100_000
MAX_OUTPUT_TOKENS: Final = 32_768
MODEL_ID_PATTERN: Final = r"^[A-Za-z0-9._:/-]+$"


class WireModel(BaseModel):
    """Frozen, non-coercing inbound contract that ignores unknown fields."""

    model_config = ConfigDict(extra="ignore", frozen=True, strict=True)


# --- Outbound ---------------------------------------------------------------


class InferenceOptions(StrictModel):
    """Generation options shared by every request of a runner."""

    temperature: float | None = Field(default=None, ge=0.0, le=2.0)
    max_tokens: int | None = Field(default=None, ge=1, le=MAX_OUTPUT_TOKENS)
    model: str | None = Field(default=None, min_length=1, max_length=128, pattern=MODEL_ID_PATTERN)


class InferenceRequest(InferenceOptions):
    """Body of ``POST /v1/inference``; the client always sets ``stream: true``."""

    prompt: str = Field(min_length=1, max_length=MAX_PROMPT_CHARS)

    @field_validator("prompt")
    @classmethod
    def _prompt_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("prompt must not be blank")
        return value

    def to_payload(self) -> dict[str, Any]:
        """JSON body for a streaming request (unset options omitted)."""
        return {**self.model_dump(mode="json", exclude_none=True), "stream": True}


# --- Inbound ----------------------------------------------------------------


class StreamDelta(WireModel):
    """``{"type": "delta"}``: an incremental piece of generated text."""

    type: Literal["delta"]
    content: str = Field(max_length=MAX_PROMPT_CHARS)


class StreamSummary(WireModel):
    """``{"type": "summary"}``: final metadata, sent once before ``[DONE]``."""

    type: Literal["summary"]
    model_used: str = Field(min_length=1, max_length=128, pattern=MODEL_ID_PATTERN)
    provider: str = Field(min_length=1, max_length=64)
    tokens_used: int = Field(ge=0)
    cached_hit: bool
    latency_ms: FiniteFloat = Field(ge=0.0)


class ErrorDetail(WireModel):
    """The gateway's public error payload."""

    code: str = Field(min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_]*$")
    message: str = Field(max_length=8192)
    retryable: bool = False


class StreamErrorFrame(WireModel):
    """``{"type": "error"}``: failure after the stream had started."""

    type: Literal["error"]
    error: ErrorDetail


class ErrorResponse(WireModel):
    """Body of every non-success HTTP response: ``{"error": {...}}``."""

    error: ErrorDetail


type StreamEvent = StreamDelta | StreamSummary


class RateLimitInfo(StrictModel):
    """Parsed ``X-RateLimit-*`` (and, on 429, ``Retry-After``) response headers."""

    limit: int = Field(ge=0)
    remaining: int = Field(ge=0)
    reset_at: AwareDatetime
    retry_after_s: FiniteFloat | None = Field(default=None, ge=0.0)


class InferenceResult(StrictModel):
    """A fully consumed stream: concatenated content plus its summary."""

    content: str
    summary: StreamSummary
    rate_limit: RateLimitInfo | None = None


def utc_from_timestamp(seconds: int) -> datetime:
    """Convert a Unix timestamp header value to an aware UTC datetime."""
    return datetime.fromtimestamp(seconds, tz=UTC)
