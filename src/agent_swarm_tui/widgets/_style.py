"""Shared presentation helpers: status badges, role colours and formatting.

Everything returns ``rich.text.Text`` built from plain strings, never markup:
agent names, tasks and model output are untrusted and must not be able to
inject styles or links.
"""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from rich.text import Text

from agent_swarm_tui.schemas import AgentRole, AgentStatus

ELLIPSIS: Final = "…"

STATUS_GLYPHS: Final[Mapping[AgentStatus, str]] = MappingProxyType(
    {
        AgentStatus.IDLE: "○",  # white circle
        AgentStatus.RUNNING: "▶",  # play
        AgentStatus.WAITING: "◷",  # clock-ish quarter circle
        AgentStatus.ERROR: "✖",  # heavy x
        AgentStatus.TERMINATED: "■",  # black square
    }
)

STATUS_STYLES: Final[Mapping[AgentStatus, str]] = MappingProxyType(
    {
        AgentStatus.IDLE: "grey70",
        AgentStatus.RUNNING: "bold green3",
        AgentStatus.WAITING: "bold yellow3",
        AgentStatus.ERROR: "bold red3",
        AgentStatus.TERMINATED: "grey50",
    }
)

ROLE_STYLES: Final[Mapping[AgentRole, str]] = MappingProxyType(
    {
        AgentRole.ORCHESTRATOR: "bold magenta",
        AgentRole.PLANNER: "bold cyan",
        AgentRole.RESEARCHER: "bold dodger_blue2",
        AgentRole.EXECUTOR: "bold green3",
        AgentRole.REVIEWER: "bold orange3",
    }
)


def status_badge(status: AgentStatus) -> Text:
    """``<glyph> STATUS`` in the status colour."""
    return Text(f"{STATUS_GLYPHS[status]} {status.value.upper()}", style=STATUS_STYLES[status])


def one_line(text: str, width: int) -> str:
    """Collapse whitespace and clip to ``width`` characters (with an ellipsis)."""
    flat = " ".join(text.split())
    if len(flat) <= width:
        return flat
    return flat[: max(0, width - 1)] + ELLIPSIS


def format_count(value: int) -> str:
    """Thousands-separated integer."""
    return f"{value:,}"


def format_cost(usd: float) -> str:
    """Dollar amount with enough precision for per-token prices."""
    return f"${usd:,.4f}" if usd < 1 else f"${usd:,.2f}"


def format_duration(seconds: float) -> str:
    """Compact countdown: ``42s``, ``3m 05s``, ``1h 02m``."""
    total = max(0, int(seconds))
    hours, rest = divmod(total, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"
