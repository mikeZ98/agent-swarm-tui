"""Modal dialog for dispatching a task to the swarm."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar
from uuid import UUID

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding, BindingType
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label, Select, Static

from agent_swarm_tui.schemas import MAX_PAYLOAD_CHARS, AgentState
from agent_swarm_tui.widgets._style import ROLE_STYLES


@dataclass(frozen=True, slots=True)
class DispatchRequest:
    """What the user asked to dispatch, and to which agent."""

    task: str
    agent_id: UUID


class DispatchDialog(ModalScreen[DispatchRequest | None]):
    """Task input plus target-agent picker; ``Enter`` dispatches, ``Esc`` cancels.

    Dismisses with a ``DispatchRequest``, or ``None`` when cancelled.
    """

    DEFAULT_CSS: ClassVar[str] = """
    DispatchDialog {
        align: center middle;
        background: $background 60%;
    }
    DispatchDialog > #dialog {
        width: 90;
        max-width: 95%;
        height: auto;
        padding: 1 2;
        border: thick $accent;
        background: $surface;
    }
    DispatchDialog #title {
        text-style: bold;
        margin-bottom: 1;
    }
    DispatchDialog #target {
        margin-top: 1;
    }
    DispatchDialog #error {
        color: $error;
        height: auto;
        display: none;
    }
    DispatchDialog #buttons {
        height: auto;
        margin-top: 1;
        align-horizontal: right;
    }
    DispatchDialog #buttons Button {
        margin-left: 2;
    }
    """

    BINDINGS: ClassVar[list[BindingType]] = [Binding("escape", "cancel", "Cancel")]

    def __init__(
        self,
        agents: Sequence[AgentState],
        *,
        entry_point: UUID,
        default_task: str = "",
    ) -> None:
        """Offer every agent as a target, preselecting ``entry_point``."""
        super().__init__()
        self._agents = tuple(agents)
        self._entry_point = entry_point
        self._default_task = default_task

    def compose(self) -> ComposeResult:
        """Title, task input, target picker, error line and buttons."""
        options: list[tuple[Text, str]] = []
        for agent in self._agents:
            label = Text(agent.name, style=ROLE_STYLES[agent.role])
            label.append(f" ({agent.role.value})", style="dim")
            if agent.id == self._entry_point:
                label.append("  default entry point", style="dim italic")
            options.append((label, str(agent.id)))
        with Vertical(id="dialog"):
            yield Label("Dispatch a task to the swarm", id="title")
            yield Input(
                value=self._default_task,
                placeholder="Describe the task…",
                max_length=MAX_PAYLOAD_CHARS,
                id="task",
            )
            yield Select(options, value=str(self._entry_point), allow_blank=False, id="target")
            yield Static(id="error")
            with Horizontal(id="buttons"):
                yield Button("Cancel", id="cancel")
                yield Button("Dispatch", variant="primary", id="dispatch")

    def on_mount(self) -> None:
        """Start typing straight away."""
        self.query_one("#task", Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """``Enter`` in the task field dispatches."""
        event.stop()
        self._submit()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Dispatch or cancel."""
        event.stop()
        if event.button.id == "dispatch":
            self._submit()
        else:
            self.dismiss(None)

    def action_cancel(self) -> None:
        """Close without dispatching."""
        self.dismiss(None)

    def _submit(self) -> None:
        task = self.query_one("#task", Input).value
        if not task.strip():
            error = self.query_one("#error", Static)
            error.update(Text("The task must not be empty."))
            error.display = True
            return
        target = self.query_one("#target", Select).value
        agent_id = UUID(target) if isinstance(target, str) else self._entry_point
        self.dismiss(DispatchRequest(task=task, agent_id=agent_id))
