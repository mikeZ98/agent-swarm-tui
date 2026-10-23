"""``SwarmApp``: the 4-panel Textual application, plus the CLI entrypoint.

Layout (responsive; stacks vertically on narrow terminals)::

    +----------------------+-------------------------------+
    | Agents (tree)        | Stream (live tokens, turns)   |
    |                      |                               |
    +----------------------+-------------------------------+
    | Telemetry            | Alerts                        |
    +----------------------+-------------------------------+

Every orchestrator event, token delta and gateway result is posted to the
app as a Textual message, so widgets update in one FIFO order on the UI
loop. Shutdown: ``q`` drains the swarm, a second ``q`` cancels in-flight
work, a third exits at once; ``on_unmount`` hard-stops the swarm on any
other exit path so no agent task outlives the app.
"""

import argparse
import asyncio
import logging
import sys
from collections.abc import Sequence
from typing import ClassVar, Final
from uuid import UUID

from textual.app import App, ComposeResult
from textual.binding import Binding, BindingType
from textual.containers import Horizontal, Vertical
from textual.logging import TextualHandler
from textual.message import Message
from textual.reactive import reactive
from textual.screen import ModalScreen
from textual.widgets import Footer, Header

from agent_swarm_tui.client import (
    GatewayAgentRunner,
    GatewayClient,
    GatewayClientSettings,
    InferenceOptions,
    RunnerOutput,
)
from agent_swarm_tui.core import (
    DEFAULT_DRAIN_TIMEOUT,
    AgentRunner,
    SwarmError,
    SwarmEvent,
    SwarmOrchestrator,
    TaskCompleted,
)
from agent_swarm_tui.presets import build_topology, reply_factory_for
from agent_swarm_tui.schemas import RoutingStrategy, SwarmTopology
from agent_swarm_tui.widgets import (
    AgentTree,
    AlertsPanel,
    DispatchDialog,
    DispatchRequest,
    StreamingLog,
    TokenTracker,
)

logger = logging.getLogger(__name__)

EXIT_OK: Final = 0
EXIT_SWARM_FAILED: Final = 1
EXIT_USAGE: Final = 2
_MODAL_BLOCKED: Final = frozenset({"dispatch", "toggle_scroll", "clear"})


class SwarmApp(App[None]):
    """Observe and steer a running swarm.

    Pass ``client`` (owned: closed on exit) to use the gateway runner, or
    ``runner`` to drive the swarm with any other ``AgentRunner`` (then no
    token deltas or gateway telemetry are shown).
    """

    TITLE = "agent-swarm-tui"

    CSS: ClassVar[str] = """
    #main { height: 1fr; }
    #left { width: 64; }
    #right { width: 1fr; }
    #agents { height: 1fr; }
    #telemetry { height: auto; }
    #stream { height: 2fr; }
    #alerts { height: 1fr; min-height: 6; }
    Screen.-wide #left { width: 76; }
    Screen.-narrow #main { layout: vertical; }
    Screen.-narrow #left { width: 1fr; height: 2fr; }
    Screen.-narrow #right { height: 3fr; }
    """

    # Textual declares this as `ClassVar[...] | None`, which mypy reads as an instance
    # variable, so a ClassVar annotation here would be rejected.
    HORIZONTAL_BREAKPOINTS = [  # noqa: RUF012
        (0, "-narrow"),
        (120, "-normal"),
        (170, "-wide"),
    ]

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding("q", "quit", "Quit"),
        Binding("d", "dispatch", "Dispatch task"),
        Binding("s", "toggle_scroll", "Scroll lock"),
        Binding("c", "clear", "Clear"),
    ]

    swarm_status: reactive[str] = reactive("starting")
    """``starting`` / ``running`` / ``draining`` / ``cancelling`` / ``stopped`` / ``crashed``."""

    class SwarmEventReceived(Message):
        """An orchestrator event, marshalled onto the UI loop."""

        def __init__(self, event: SwarmEvent) -> None:
            """Wrap ``event``."""
            super().__init__()
            self.event = event

    class TokenStreamed(Message):
        """A streamed token fragment from the gateway runner."""

        def __init__(self, agent_id: UUID, text: str) -> None:
            """Wrap one delta."""
            super().__init__()
            self.agent_id = agent_id
            self.text = text

    class GatewayCallFinished(Message):
        """One completed gateway call (tokens, latency, rate limit)."""

        def __init__(self, output: RunnerOutput) -> None:
            """Wrap ``output``."""
            super().__init__()
            self.output = output

    def __init__(
        self,
        topology: SwarmTopology,
        *,
        client: GatewayClient | None = None,
        runner: AgentRunner | None = None,
        options: InferenceOptions | None = None,
        cost_per_million_tokens: float = 0.0,
        initial_task: str | None = None,
        drain_timeout: float = DEFAULT_DRAIN_TIMEOUT,
    ) -> None:
        """Build the swarm (not started until the app mounts)."""
        super().__init__()
        self._topology = topology
        self._client: GatewayClient | None = None
        if runner is None:
            self._client = client if client is not None else GatewayClient()
            runner = GatewayAgentRunner(
                self._client,
                options=options,
                reply_factory=reply_factory_for(topology.routing_strategy),
                on_delta=self._post_delta,
                on_output=self._post_output,
            )
        self._swarm = SwarmOrchestrator(
            topology,
            runner,
            listener=self._post_event,
            cost_per_million_tokens=cost_per_million_tokens,
        )
        self._initial_task = initial_task
        self._drain_timeout = drain_timeout
        self._quit_event = asyncio.Event()
        self._quit_requests = 0
        self._shutdown_requested = False
        self._lifecycle_done = False
        self._client_closed = False
        self._exit_code = EXIT_OK

    @property
    def swarm(self) -> SwarmOrchestrator:
        """The orchestrator this app drives."""
        return self._swarm

    # --- Layout & startup --------------------------------------------------------

    def compose(self) -> ComposeResult:
        """Header, the four panels and the key-binding footer."""
        yield Header(show_clock=True)
        with Horizontal(id="main"):
            with Vertical(id="left"):
                yield AgentTree(self._topology, id="agents")
                yield TokenTracker(id="telemetry")
            with Vertical(id="right"):
                yield StreamingLog(id="stream")
                yield AlertsPanel(id="alerts")
        yield Footer()

    def on_mount(self) -> None:
        """Wire widgets to the topology and launch the swarm in a worker."""
        for widget in (self._stream, self._tracker, self._alerts):
            widget.load_topology(self._topology)
        self._update_subtitle()
        self.query_one(AgentTree).focus()
        self.set_interval(1.0, self._tick)
        self.run_worker(
            self._lifecycle(), name="swarm-lifecycle", group="swarm", exit_on_error=False
        )

    async def _lifecycle(self) -> None:
        try:
            await self._swarm.start_swarm()
        except SwarmError as exc:
            self._fail(exc)
            self._lifecycle_done = True
            return
        self.swarm_status = "running"
        where = f" via {self._client.settings.base_url}" if self._client is not None else ""
        self._alerts.info(f"swarm {self._topology.name!r} started{where}")
        if self._initial_task:
            self._dispatch(self._initial_task, None)
        try:
            await self._quit_event.wait()
            self.swarm_status = "draining"
            drained = await self._swarm.stop_swarm(drain=True, drain_timeout=self._drain_timeout)
            self.swarm_status = "stopped"
            if not drained:
                self._alerts.info("in-flight work was cancelled")
        except SwarmError as exc:
            self._fail(exc)
        finally:
            await self._close_client()
            self._lifecycle_done = True
        self.exit(return_code=self._exit_code)

    async def on_unmount(self) -> None:
        """Hard-stop the swarm on any exit path (no-op after a graceful quit)."""
        try:
            await self._swarm.stop_swarm(drain=False)
        except SwarmError:
            logger.exception("swarm crashed during shutdown")
        await self._close_client()

    # --- Events from the swarm ---------------------------------------------------------

    def _post_event(self, event: SwarmEvent) -> None:
        self.post_message(self.SwarmEventReceived(event))

    def _post_delta(self, agent_id: UUID, text: str) -> None:
        self.post_message(self.TokenStreamed(agent_id, text))

    def _post_output(self, output: RunnerOutput) -> None:
        self.post_message(self.GatewayCallFinished(output))

    def on_swarm_app_swarm_event_received(self, message: SwarmEventReceived) -> None:
        """Fan an orchestrator event out to the panels."""
        event = message.event
        self.query_one(AgentTree).apply_event(event)
        self._stream.apply_event(event)
        self._alerts.apply_event(event)
        self._tracker.update_metrics(self._swarm.metrics)
        if isinstance(event, TaskCompleted):
            name = self._swarm.agent(event.agent_id).name
            self.notify(f"Result from {name}", title="Task completed", timeout=4)

    def on_swarm_app_token_streamed(self, message: TokenStreamed) -> None:
        """Stream a token delta into the log."""
        self._stream.append_delta(message.agent_id, message.text)

    def on_swarm_app_gateway_call_finished(self, message: GatewayCallFinished) -> None:
        """Count a gateway call in the telemetry panel."""
        self._tracker.record_output(message.output)

    def on_agent_tree_agent_selected(self, message: AgentTree.AgentSelected) -> None:
        """Focus the stream on the selected agent (root: whole swarm)."""
        self._stream.focus_agent(message.agent_id)

    # --- Actions -------------------------------------------------------------------

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        """Block panel actions behind modals; grey out dispatch when not running."""
        if action in _MODAL_BLOCKED and isinstance(self.screen, ModalScreen):
            return False
        if action == "dispatch" and (self._shutdown_requested or not self._swarm.is_running):
            return None
        return True

    def action_dispatch(self) -> None:
        """Open the dispatch dialog."""
        dialog = DispatchDialog(self._swarm.agents, entry_point=self._swarm.entry_point)
        self.push_screen(dialog, callback=self._on_dispatch_result)

    def action_toggle_scroll(self) -> None:
        """Toggle the stream's scroll lock."""
        self._stream.action_toggle_auto_scroll()

    def action_clear(self) -> None:
        """Clear the stream history and the alerts."""
        self._stream.action_clear_log()
        self._alerts.clear_alerts()

    async def action_quit(self) -> None:
        """Drain and quit; again: cancel in-flight work; a third time: exit now."""
        if isinstance(self.screen, DispatchDialog):
            self.screen.dismiss(None)
        if self._lifecycle_done:
            self.exit(return_code=self._exit_code)
            return
        self._quit_requests += 1
        if self._quit_requests == 1:
            self._shutdown_requested = True
            self._quit_event.set()
            self._alerts.info("draining the swarm; press q again to cancel in-flight work")
        elif self._quit_requests == 2:  # noqa: PLR2004 - second press
            self.swarm_status = "cancelling"
            self._swarm.abort_drain()
            self._alerts.info("cancelling in-flight work")
        else:
            self.exit(return_code=self._exit_code)
        self.refresh_bindings()

    # --- Internals ---------------------------------------------------------------------

    @property
    def _stream(self) -> StreamingLog:
        return self.query_one(StreamingLog)

    @property
    def _tracker(self) -> TokenTracker:
        return self.query_one(TokenTracker)

    @property
    def _alerts(self) -> AlertsPanel:
        return self.query_one(AlertsPanel)

    def _on_dispatch_result(self, result: DispatchRequest | None) -> None:
        if result is not None:
            self._dispatch(result.task, result.agent_id)

    def _dispatch(self, task: str, agent_id: UUID | None) -> None:
        try:
            self._swarm.dispatch_task(task, agent_id=agent_id)
        except (SwarmError, ValueError) as exc:
            self._alerts.notify_exception(exc, agent_id=agent_id)
            self.notify("Task was not dispatched; see Alerts.", severity="error")

    def _fail(self, exc: SwarmError) -> None:
        self.swarm_status = "crashed"
        self._exit_code = EXIT_SWARM_FAILED
        self._alerts.notify_exception(exc)
        logger.error("swarm failed", exc_info=exc)

    def _tick(self) -> None:
        self._tracker.update_metrics(self._swarm.metrics)
        if self._client is not None and self._client.last_rate_limit is not None:
            self._tracker.update_rate_limit(self._client.last_rate_limit)
        if self.swarm_status == "running" and not self._swarm.is_running:
            self.swarm_status = "crashed"
            self._exit_code = EXIT_SWARM_FAILED
            self._alerts.info("the swarm stopped unexpectedly; press q to exit")

    async def _close_client(self) -> None:
        if self._client is not None and not self._client_closed:
            self._client_closed = True
            await self._client.aclose()

    def watch_swarm_status(self) -> None:
        """Reflect the lifecycle in the header and the footer bindings."""
        self._update_subtitle()
        self.refresh_bindings()

    def _update_subtitle(self) -> None:
        t = self._topology
        self.sub_title = (
            f"{t.name} · {t.routing_strategy.value} · {len(t.agents)} agents · {self.swarm_status}"
        )


# --- CLI ---------------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """Command-line interface of ``agent-swarm-tui``."""
    from agent_swarm_tui import __version__  # noqa: PLC0415 - avoid import cycle

    parser = argparse.ArgumentParser(
        prog="agent-swarm-tui",
        description="Observe and steer a multi-agent swarm running on ai-inference-gateway.",
    )
    parser.add_argument(
        "--gateway-url",
        metavar="URL",
        help="gateway base URL (default: $GATEWAY_BASE_URL or http://127.0.0.1:8000)",
    )
    parser.add_argument(
        "--topology",
        choices=[s.value for s in RoutingStrategy],
        default=RoutingStrategy.SEQUENTIAL.value,
        help="preset swarm and routing strategy (default: %(default)s)",
    )
    parser.add_argument("--name", default="swarm", help="swarm name (default: %(default)s)")
    parser.add_argument(
        "--task", metavar="TEXT", help="dispatch this task as soon as the swarm starts"
    )
    parser.add_argument("--model", help="explicit model override sent to the gateway")
    parser.add_argument("--temperature", type=float, help="sampling temperature (0-2)")
    parser.add_argument("--max-tokens", type=int, help="max output tokens per agent turn")
    parser.add_argument(
        "--cost-per-million",
        type=float,
        default=0.0,
        metavar="USD",
        help="price per million tokens for the cost estimate (default: %(default)s)",
    )
    parser.add_argument(
        "--drain-timeout",
        type=float,
        default=DEFAULT_DRAIN_TIMEOUT,
        metavar="SECONDS",
        help="how long q waits for in-flight work before cancelling (default: %(default)s)",
    )
    parser.add_argument(
        "--log-file", metavar="PATH", help="write logs here (default: Textual devtools)"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="WARNING",
        help="(default: %(default)s)",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def _configure_logging(log_file: str | None, level: str) -> None:
    # Never let records reach stderr: that would corrupt the terminal UI.
    handler: logging.Handler = (
        logging.FileHandler(log_file, encoding="utf-8") if log_file else TextualHandler()
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logging.basicConfig(level=level, handlers=[handler], force=True)


def build_app(args: argparse.Namespace) -> SwarmApp:
    """Turn parsed CLI arguments into a ready (not yet running) ``SwarmApp``.

    Raises ``ValueError`` (including Pydantic ``ValidationError``) for invalid
    settings, e.g. a malformed ``--gateway-url`` or an out-of-range option.
    """
    if args.task is not None and not args.task.strip():
        raise ValueError("--task must not be blank")
    settings = (
        GatewayClientSettings(base_url=args.gateway_url)
        if args.gateway_url
        else GatewayClientSettings()
    )
    options = InferenceOptions(
        model=args.model, temperature=args.temperature, max_tokens=args.max_tokens
    )
    return SwarmApp(
        build_topology(RoutingStrategy(args.topology), name=args.name),
        client=GatewayClient(settings),
        options=options,
        cost_per_million_tokens=args.cost_per_million,
        initial_task=args.task,
        drain_timeout=args.drain_timeout,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the TUI; return the process exit code."""
    args = build_parser().parse_args(argv)
    try:
        app = build_app(args)
    except (ValueError, SwarmError) as exc:
        print(f"agent-swarm-tui: invalid configuration: {exc}", file=sys.stderr)
        return EXIT_USAGE
    _configure_logging(args.log_file, args.log_level)
    app.run()
    return app.return_code if app.return_code is not None else EXIT_OK
