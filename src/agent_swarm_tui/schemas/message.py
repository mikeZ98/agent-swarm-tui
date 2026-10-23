"""Inter-agent message contracts."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Final, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from agent_swarm_tui.schemas.base import StrictModel

MAX_PAYLOAD_CHARS: Final = 65_536


class MessageType(StrEnum):
    """Delivery semantics of a ``SwarmMessage``."""

    BROADCAST = "broadcast"
    """Delivered to every agent except the sender; no ``target_id``."""
    DIRECT = "direct"
    """Delivered to exactly one other agent; ``target_id`` required."""
    SYSTEM = "system"
    """Control-plane message from the runtime; ``target_id`` optional (None = all)."""


class SwarmMessage(StrictModel):
    """A single message on the swarm bus."""

    sender_id: UUID
    target_id: UUID | None = None
    payload: str = Field(min_length=1, max_length=MAX_PAYLOAD_CHARS)
    timestamp: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    message_type: MessageType

    @model_validator(mode="after")
    def _target_matches_type(self) -> Self:
        if self.message_type is MessageType.DIRECT:
            if self.target_id is None:
                raise ValueError("direct messages require a target_id")
            if self.target_id == self.sender_id:
                raise ValueError("direct messages cannot target their sender")
        elif self.message_type is MessageType.BROADCAST and self.target_id is not None:
            raise ValueError("broadcast messages must not set a target_id")
        return self
