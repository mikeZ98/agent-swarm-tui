"""Aggregate swarm telemetry contracts."""

from pydantic import Field, FiniteFloat

from agent_swarm_tui.schemas.base import StrictModel


class SwarmMetrics(StrictModel):
    """Snapshot of swarm-wide counters (monotonic except ``active_agents``)."""

    active_agents: int = Field(default=0, ge=0)
    messages_processed: int = Field(default=0, ge=0)
    total_tokens: int = Field(default=0, ge=0)
    estimated_cost_usd: FiniteFloat = Field(default=0.0, ge=0.0)
