"""Streaming log: live token deltas plus a turn-by-turn history of the swarm.

Two kinds of history entry are kept visually distinct:

- **thoughts**: an agent's streamed output, under a rule naming the agent;
- **messages**: one dim line per routed message (``sender -> recipients``).

In-flight deltas render in a live area under the history (repainted at most
``LIVE_REFRESH_HZ`` times per second) and move into the history when the
agent's turn ends. ``focus_agent`` filters both to a single agent.
"""

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Final
from uuid import UUID

from rich.console import Group, RenderableType
from rich.rule import Rule
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding, BindingType
from textual.reactive import var
from textual.widget import Widget
from textual.widgets import RichLog, Static

from agent_swarm_tui.core import AgentFailed, AgentUpdated, MessageRouted, SwarmEvent, TaskCompleted
from agent_swarm_tui.schemas import AgentRole, AgentStatus, MessageType, SwarmTopology
from agent_swarm_tui.widgets._style import ROLE_STYLES, one_line

MAX_HISTORY_ENTRIES: Final = 2000
MAX_LIVE_CHARS: Final = 4000
MESSAGE_SNIPPET_CHARS: Final = 96
LIVE_REFRESH_HZ: Final = 15

_TURN_OVER: Final = frozenset({AgentStatus.IDLE, AgentStatus.ERROR, AgentStatus.TERMINATED})


@dataclass(frozen=True, slots=True)
class _Entry:
    agents: frozenset[UUID]
    renderable: RenderableType


class StreamingLog(Widget):
    """Live view of agent output and inter-agent traffic.

    Feed it ``append_delta`` (from the runner's ``on_delta``) and swarm events
    via ``apply_event``. Press ``s`` to toggle auto-scroll (scroll lock).
    """

    DEFAULT_CSS: ClassVar[str] = """
    StreamingLog {
        border: round $accent;
        border-title-color: $accent;
        layout: vertical;
    }
    StreamingLog > RichLog {
        height: 1fr;
        background: transparent;
        scrollbar-size-vertical: 1;
    }
    StreamingLog > #live {
        height: auto;
        max-height: 50%;
        padding: 0 1;
        border-top: dashed $accent;
        display: none;
    }
    """

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding("s", "toggle_auto_scroll", "Scroll lock"),
        Binding("c", "clear_log", "Clear log", show=False),
    ]

    auto_scroll: var[bool] = var(default=True)
    """Follow new output; turning it off freezes the view where it is."""

    def __init__(
        self,
        *,
        name: str | None = None,
        id: str | None = None,  # noqa: A002 - Textual's widget API
        classes: str | None = None,
    ) -> None:
        """Create an empty log."""
        super().__init__(name=name, id=id, classes=classes)
        self._entries: deque[_Entry] = deque(maxlen=MAX_HISTORY_ENTRIES)
        self._live: dict[UUID, list[str]] = {}
        self._last_thought: dict[UUID, str] = {}
        self._names: dict[UUID, str] = {}
        self._roles: dict[UUID, AgentRole] = {}
        self._swarm_id: UUID | None = None
        self._focus: UUID | None = None
        self._live_dirty = False

    def compose(self) -> ComposeResult:
        """History log plus the live (in-flight) area."""
        yield RichLog(wrap=True, markup=False, highlight=False, max_lines=20_000)
        yield Static(id="live")

    def on_mount(self) -> None:
        """Start the throttled live-area repaint."""
        self._update_title()
        self.set_interval(1 / LIVE_REFRESH_HZ, self._refresh_live)

    # --- Public API ------------------------------------------------------------

    @property
    def focused_agent(self) -> UUID | None:
        """Agent the view is filtered to, or ``None`` for the whole swarm."""
        return self._focus

    def load_topology(self, topology: SwarmTopology) -> None:
        """Learn agent names/roles (for labels) and the swarm's runtime id."""
        self._swarm_id = topology.swarm_id
        self._names = {agent.id: agent.name for agent in topology.agents}
        self._roles = {agent.id: agent.role for agent in topology.agents}

    def append_delta(self, agent_id: UUID, text: str) -> None:
        """Append a streamed token fragment to ``agent_id``'s in-flight turn."""
        if text:
            self._live.setdefault(agent_id, []).append(text)
            self._live_dirty = True

    def end_turn(self, agent_id: UUID) -> None:
        """Move ``agent_id``'s in-flight output into the history as a thought."""
        parts = self._live.pop(agent_id, None)
        self._live_dirty = True
        if not parts:
            return
        text = "".join(parts)
        self._last_thought[agent_id] = text
        body = Text(text)
        self._add(frozenset({agent_id}), Group(self._agent_rule(agent_id), body))

    def apply_event(self, event: SwarmEvent) -> None:
        """Consume a swarm event."""
        if isinstance(event, MessageRouted):
            if event.message.message_type is not MessageType.SYSTEM:
                self.end_turn(event.message.sender_id)
            self._add_message(event)
        elif isinstance(event, AgentUpdated):
            if event.agent.status in _TURN_OVER:
                self.end_turn(event.agent.id)
        elif isinstance(event, TaskCompleted):
            self._add_result(event)
        elif isinstance(event, AgentFailed):
            self.end_turn(event.agent_id)
            line = Text("  ✖ ", style="bold red3")
            line.append(self._agent_name(event.agent_id), style="bold red3")
            line.append(f" failed: {one_line(event.error, MESSAGE_SNIPPET_CHARS)}", style="red3")
            self._add(frozenset({event.agent_id}), line)

    def focus_agent(self, agent_id: UUID | None) -> None:
        """Show only entries involving ``agent_id`` (``None``: everything)."""
        self._focus = agent_id
        self._update_title()
        self._rerender()

    def action_toggle_auto_scroll(self) -> None:
        """Toggle scroll lock."""
        self.auto_scroll = not self.auto_scroll

    def action_clear_log(self) -> None:
        """Drop the history (in-flight turns are kept)."""
        self._entries.clear()
        self._last_thought.clear()
        self._rerender()

    # --- Internals --------------------------------------------------------------

    def watch_auto_scroll(self, value: bool) -> None:  # noqa: FBT001 - Textual watcher
        """Propagate scroll lock to the history log and jump to the end on unlock."""
        if not self.is_mounted:
            return
        log = self.query_one(RichLog)
        log.auto_scroll = value
        if value:
            log.scroll_end(animate=False)
        self._update_title()

    def _add_message(self, event: MessageRouted) -> None:
        message = event.message
        line = Text(f"  {datetime.now().astimezone():%H:%M:%S} ", style="dim")
        if message.message_type is MessageType.SYSTEM:
            line.append("⇒ task", style="bold")
        else:
            line.append("✉ ", style="dim")
            line.append(
                self._agent_name(message.sender_id), style=self._agent_style(message.sender_id)
            )
        line.append(" → ", style="dim")
        if message.message_type is MessageType.BROADCAST:
            line.append("all", style="bold")
        else:
            for index, agent_id in enumerate(event.recipients):
                if index:
                    line.append(", ", style="dim")
                line.append(self._agent_name(agent_id), style=self._agent_style(agent_id))
        line.append(f"  {one_line(message.payload, MESSAGE_SNIPPET_CHARS)}", style="dim italic")
        involved = {message.sender_id, *event.recipients}
        self._add(frozenset(involved), line)

    def _add_result(self, event: TaskCompleted) -> None:
        self.end_turn(event.agent_id)  # flush first, so the thought precedes its result
        line = Text("  ✔ result from ", style="bold green3")
        line.append(self._agent_name(event.agent_id), style=self._agent_style(event.agent_id))
        if self._last_thought.get(event.agent_id) == event.output:
            self._add(frozenset({event.agent_id}), line)
            return
        self._add(frozenset({event.agent_id}), Group(line, Text(event.output)))

    def _add(self, agents: frozenset[UUID], renderable: RenderableType) -> None:
        entry = _Entry(agents=agents, renderable=renderable)
        self._entries.append(entry)
        if self.is_mounted and self._visible(entry):
            self._write(self.query_one(RichLog), renderable)

    @staticmethod
    def _write(log: RichLog, renderable: RenderableType) -> None:
        # expand: wrap at the panel width, not the (possibly narrower) console width.
        log.write(renderable, expand=True)

    def _visible(self, entry: _Entry) -> bool:
        return self._focus is None or self._focus in entry.agents

    def _rerender(self) -> None:
        self._live_dirty = True
        if not self.is_mounted:
            return
        log = self.query_one(RichLog)
        log.clear()
        for entry in self._entries:
            if self._visible(entry):
                self._write(log, entry.renderable)

    def _refresh_live(self) -> None:
        if not self._live_dirty:
            return
        self._live_dirty = False
        live = self.query_one("#live", Static)
        blocks: list[RenderableType] = []
        for agent_id, parts in self._live.items():
            if self._focus is not None and agent_id != self._focus:
                continue
            text = "".join(parts)
            if len(text) > MAX_LIVE_CHARS:
                text = "…" + text[-MAX_LIVE_CHARS:]
            header = Text("▶ ", style="bold green3")
            header.append(self._agent_name(agent_id), style=self._agent_style(agent_id))
            header.append("  streaming…", style="dim")
            blocks.append(Group(header, Text(text)))
        live.display = bool(blocks)
        live.update(Group(*blocks) if blocks else "")

    def _agent_rule(self, agent_id: UUID) -> Rule:
        title = Text(self._agent_name(agent_id), style=self._agent_style(agent_id))
        role = self._roles.get(agent_id)
        if role is not None:
            title.append(f" ({role.value})", style="dim")
        return Rule(title, align="left", style="dim")

    def _agent_name(self, agent_id: UUID) -> str:
        if agent_id == self._swarm_id:
            return "swarm"
        return self._names.get(agent_id, str(agent_id)[:8])

    def _agent_style(self, agent_id: UUID) -> str:
        role = self._roles.get(agent_id)
        return ROLE_STYLES[role] if role is not None else "bold"

    def _update_title(self) -> None:
        scope = "all agents" if self._focus is None else self._agent_name(self._focus)
        title = Text(f"Stream · {scope}")
        if not self.auto_scroll:
            title.append("  scroll locked", style="bold yellow3")
        self.border_title = title  # Text: never parsed as markup
