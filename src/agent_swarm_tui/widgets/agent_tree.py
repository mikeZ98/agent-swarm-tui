"""Agent tree / thread monitor: every agent with its role, status and current task.

Layout follows the routing strategy: ``HIERARCHICAL`` nests workers under the
orchestrator, ``SEQUENTIAL`` numbers the pipeline steps, ``MESH`` is flat.
Selecting an agent posts ``AgentTree.AgentSelected`` (the root selects the
whole swarm), which the app uses to focus the streaming log.
"""

from typing import ClassVar, Final
from uuid import UUID

from rich.text import Text
from textual.message import Message
from textual.widgets import Tree
from textual.widgets.tree import TreeNode

from agent_swarm_tui.core import AgentUpdated, SwarmEvent
from agent_swarm_tui.schemas import (
    AgentRole,
    AgentState,
    AgentStatus,
    RoutingStrategy,
    SwarmTopology,
)
from agent_swarm_tui.widgets._style import (
    ROLE_STYLES,
    STATUS_GLYPHS,
    STATUS_STYLES,
    format_count,
    one_line,
    status_badge,
)

TASK_SNIPPET_CHARS: Final = 36
_ACTIVE: Final = frozenset({AgentStatus.RUNNING, AgentStatus.WAITING})


def agent_label(state: AgentState, *, step: int | None = None) -> Text:
    """One-line node label: badge, name, role, a task snippet and tokens."""
    label = Text()
    if step is not None:
        label.append(f"{step}. ", style="dim")
    label.append_text(status_badge(state.status))
    label.append("  ")
    label.append(state.name, style=ROLE_STYLES[state.role])
    label.append(f" ({state.role.value})", style="dim")
    if state.current_task:  # before tokens: the task matters more in a narrow pane
        label.append("  — ", style="dim")
        label.append(one_line(state.current_task, TASK_SNIPPET_CHARS), style="italic")
    if state.total_tokens:
        label.append(f"  {format_count(state.total_tokens)} tok", style="dim cyan")
    return label


def swarm_label(topology: SwarmTopology, states: dict[UUID, AgentState]) -> Text:
    """Root label: swarm name, strategy and live status counts."""
    label = Text()
    label.append(topology.name, style="bold")
    label.append(f"  {topology.routing_strategy.value}", style="dim")
    label.append(f"  {len(states)} agents", style="dim")
    for status in (AgentStatus.RUNNING, AgentStatus.WAITING, AgentStatus.ERROR):
        count = sum(state.status is status for state in states.values())
        if count:
            label.append(f"  {STATUS_GLYPHS[status]} {count}", style=STATUS_STYLES[status])
    return label


class AgentTree(Tree[UUID]):
    """Live tree of swarm agents; feed it ``AgentUpdated`` events."""

    DEFAULT_CSS: ClassVar[str] = """
    AgentTree {
        border: round $primary;
        border-title-color: $primary;
        padding: 0 1;
    }
    """

    class AgentSelected(Message):
        """The user selected an agent (``agent_id``) or the whole swarm (``None``)."""

        def __init__(self, agent_id: UUID | None) -> None:
            """Carry the selected agent id."""
            super().__init__()
            self.agent_id = agent_id

    def __init__(
        self,
        topology: SwarmTopology | None = None,
        *,
        name: str | None = None,
        id: str | None = None,  # noqa: A002 - Textual's widget API
        classes: str | None = None,
    ) -> None:
        """Create the tree, optionally populated from ``topology``."""
        super().__init__(Text("no swarm loaded", style="dim"), name=name, id=id, classes=classes)
        self.border_title = "Agents"
        self.guide_depth = 3
        self.auto_expand = False  # selecting focuses the log; space still toggles
        self._topology: SwarmTopology | None = None
        self._states: dict[UUID, AgentState] = {}
        self._agent_nodes: dict[UUID, TreeNode[UUID]] = {}
        self._steps: dict[UUID, int] = {}
        if topology is not None:
            self.load_topology(topology)

    @property
    def states(self) -> tuple[AgentState, ...]:
        """Latest known state of every agent, in topology order."""
        return tuple(self._states.values())

    def load_topology(self, topology: SwarmTopology) -> None:
        """Rebuild the tree for ``topology`` (its agents' current states)."""
        self._topology = topology
        self._states = {agent.id: agent for agent in topology.agents}
        self._agent_nodes.clear()
        self._steps = (
            {agent.id: index for index, agent in enumerate(topology.agents, start=1)}
            if topology.routing_strategy is RoutingStrategy.SEQUENTIAL
            else {}
        )
        self.clear()
        self.root.set_label(swarm_label(topology, self._states))
        parent = self.root
        if topology.routing_strategy is RoutingStrategy.HIERARCHICAL:
            hub = next(a for a in topology.agents if a.role is AgentRole.ORCHESTRATOR)
            parent = self._agent_nodes[hub.id] = self.root.add(
                agent_label(hub), data=hub.id, expand=True
            )
        for agent in topology.agents:
            if agent.id not in self._agent_nodes:
                self._agent_nodes[agent.id] = parent.add_leaf(
                    agent_label(agent, step=self._steps.get(agent.id)), data=agent.id
                )
        self.root.expand_all()

    def update_agent(self, state: AgentState) -> None:
        """Re-render one agent (ignored for agents outside the loaded topology)."""
        node = self._agent_nodes.get(state.id)
        if node is None or self._topology is None:
            return
        self._states[state.id] = state
        node.set_label(agent_label(state, step=self._steps.get(state.id)))
        self.root.set_label(swarm_label(self._topology, self._states))

    def apply_event(self, event: SwarmEvent) -> None:
        """Consume a swarm event (only ``AgentUpdated`` affects the tree)."""
        if isinstance(event, AgentUpdated):
            self.update_agent(event.agent)

    def on_tree_node_selected(self, event: Tree.NodeSelected[UUID]) -> None:
        """Translate node selection into ``AgentSelected``."""
        event.stop()
        self.post_message(self.AgentSelected(event.node.data))
