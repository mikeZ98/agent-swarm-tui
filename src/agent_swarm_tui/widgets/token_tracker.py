"""Telemetry panel: swarm metrics, gateway call stats and rate-limit headroom."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import ClassVar, Final

from rich.console import RenderableType
from rich.table import Table
from rich.text import Text
from textual.reactive import reactive
from textual.widget import Widget

from agent_swarm_tui.client import RateLimitInfo, RunnerOutput
from agent_swarm_tui.schemas import SwarmMetrics, SwarmTopology
from agent_swarm_tui.widgets._style import format_cost, format_count, format_duration, one_line

HEADROOM_BAR_CELLS: Final = 10
LABEL_CELLS: Final = 11
_HEALTHY: Final = 0.5
_LOW: Final = 0.2


def _utcnow() -> datetime:
    return datetime.now(UTC)


def headroom_bar(remaining: int, limit: int, *, cells: int = HEADROOM_BAR_CELLS) -> Text:
    """``remaining/limit`` as a coloured bar (green > 50%, yellow > 20%, else red)."""
    ratio = remaining / limit if limit > 0 else 0.0
    ratio = min(1.0, max(0.0, ratio))
    filled = round(ratio * cells)
    style = "green3" if ratio > _HEALTHY else "yellow3" if ratio > _LOW else "red3"
    bar = Text("█" * filled, style=style)
    bar.append("░" * (cells - filled), style="grey35")
    return bar


class TokenTracker(Widget):
    """Live ``SwarmMetrics`` plus gateway telemetry.

    Call ``update_metrics`` (e.g. on a timer or per event), ``record_output``
    from the runner's ``on_output``, and ``update_rate_limit`` with
    ``GatewayClient.last_rate_limit``. The reset countdown ticks every second.
    """

    DEFAULT_CSS: ClassVar[str] = """
    TokenTracker {
        border: round $secondary;
        border-title-color: $secondary;
        padding: 0 1;
        height: auto;
    }
    """

    metrics: reactive[SwarmMetrics] = reactive(SwarmMetrics)
    total_agents: reactive[int] = reactive(0)
    # These two add/remove rows, so they re-run layout (``height: auto``).
    rate_limit: reactive[RateLimitInfo | None] = reactive(None, layout=True)
    last_output: reactive[RunnerOutput | None] = reactive(None, layout=True)

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] = _utcnow,
        name: str | None = None,
        id: str | None = None,  # noqa: A002 - Textual's widget API
        classes: str | None = None,
    ) -> None:
        """Create the panel; ``clock`` (aware UTC) drives the reset countdown."""
        super().__init__(name=name, id=id, classes=classes)
        self.border_title = "Telemetry"
        self._clock = clock
        self._calls = 0
        self._cache_hits = 0

    def on_mount(self) -> None:
        """Tick the rate-limit countdown."""
        self.set_interval(1.0, self._tick)

    @property
    def gateway_calls(self) -> int:
        """Gateway calls recorded via ``record_output``."""
        return self._calls

    @property
    def cache_hits(self) -> int:
        """Calls the gateway served from its semantic cache."""
        return self._cache_hits

    def load_topology(self, topology: SwarmTopology) -> None:
        """Set the agent total shown next to the active count."""
        self.total_agents = len(topology.agents)

    def update_metrics(self, metrics: SwarmMetrics) -> None:
        """Show a new metrics snapshot."""
        self.metrics = metrics

    def update_rate_limit(self, info: RateLimitInfo | None) -> None:
        """Show the gateway's latest rate-limit state (``None``: not reported)."""
        self.rate_limit = info

    def record_output(self, output: RunnerOutput) -> None:
        """Count one gateway call and pick up its rate-limit headers, if any."""
        self._calls += 1
        self._cache_hits += output.cached_hit
        self.last_output = output
        if output.rate_limit is not None:
            self.rate_limit = output.rate_limit
        self.refresh()

    def render(self) -> RenderableType:
        """Two-column metrics grid (labels fixed, values clipped to fit)."""
        m = self.metrics
        grid = Table.grid(padding=(0, 2))
        grid.add_column(style="dim", no_wrap=True, min_width=LABEL_CELLS)
        grid.add_column(no_wrap=True, overflow="ellipsis")
        active = Text(str(m.active_agents), style="bold green3" if m.active_agents else "")
        if self.total_agents:
            active.append(f" / {self.total_agents}", style="dim")
        grid.add_row("Active", active)
        grid.add_row("Messages", Text(format_count(m.messages_processed)))
        grid.add_row("Tokens", Text(format_count(m.total_tokens), style="bold cyan"))
        grid.add_row("Cost (est.)", Text(format_cost(m.estimated_cost_usd), style="bold"))
        grid.add_row("Calls", self._calls_text())
        last = self.last_output
        if last is not None:
            grid.add_row(
                "Last call",
                Text(
                    f"{one_line(last.provider, 16)}/{one_line(last.model_used, 32)}"
                    f" \u00b7 {last.latency_ms:,.0f} ms",
                ),
            )
        for label, value in self.rate_limit_rows():
            grid.add_row(label, value)
        return grid

    def rate_limit_rows(self) -> list[tuple[str, Text]]:
        """Headroom row plus, when reported, the window (reset / retry) row."""
        info = self.rate_limit
        if info is None:
            return [("Rate limit", Text("not reported", style="dim"))]
        headroom = headroom_bar(info.remaining, info.limit)
        headroom.append(f" {info.remaining}/{info.limit}")
        seconds = (info.reset_at - self._clock()).total_seconds()
        if info.remaining == 0 and info.retry_after_s is not None:
            window = ("Retry in", Text(format_duration(info.retry_after_s), style="bold red3"))
        elif seconds > 0:
            window = ("Resets in", Text(format_duration(seconds)))
        else:
            window = ("Window", Text("reset", style="dim"))
        return [("Rate limit", headroom), window]

    def _calls_text(self) -> Text:
        text = Text(format_count(self._calls))
        if self._calls:
            share = self._cache_hits / self._calls
            text.append(f" \u00b7 {self._cache_hits} cached ({share:.0%})", style="dim")
        return text

    def _tick(self) -> None:
        if self.rate_limit is not None:
            self.refresh()
