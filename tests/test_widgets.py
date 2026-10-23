"""Widget rendering, markup safety, custom events and reactive updates."""

import io
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError
from rich.console import Console
from rich.text import Text
from textual.app import App, ComposeResult
from textual.widgets import RichLog, Static

from agent_swarm_tui.client import (
    GatewayClientError,
    GatewayConnectionError,
    GatewayRateLimitError,
    GatewayTimeoutError,
    RateLimitInfo,
    RunnerOutput,
)
from agent_swarm_tui.core import (
    AgentFailed,
    AgentUpdated,
    InvalidStateTransitionError,
    MessageRouted,
    RoutingError,
    TaskCompleted,
    transition,
)
from agent_swarm_tui.schemas import (
    AgentRole,
    AgentState,
    AgentStatus,
    MessageType,
    RoutingStrategy,
    SwarmMessage,
    SwarmTopology,
)
from agent_swarm_tui.widgets import (
    AgentTree,
    Alert,
    AlertCategory,
    AlertSeverity,
    AlertsPanel,
    StreamingLog,
    TokenTracker,
    agent_label,
    alert_from_exception,
    alert_from_failure,
    alert_line,
    categorize,
    headroom_bar,
    swarm_label,
)
from agent_swarm_tui.widgets._style import format_cost, format_duration, one_line, status_badge
from agent_swarm_tui.widgets.streaming_log import MAX_LIVE_CHARS
from tests.conftest import make_agent, make_topology

S = AgentStatus
EVIL = "[bold red]PWN[/][link=http://x]click[/link]"


def follows(log: StreamingLog) -> bool:
    return log.auto_scroll  # read through a function: mypy would narrow the attribute


def lines(log: RichLog) -> str:
    return "\n".join("".join(segment.text for segment in strip) for strip in log.lines)


def routed(
    sender: AgentState | SwarmTopology,
    kind: MessageType,
    *recipients: AgentState,
    payload: str = "hi",
) -> MessageRouted:
    sender_id = sender.swarm_id if isinstance(sender, SwarmTopology) else sender.id
    target = recipients[0].id if kind is not MessageType.BROADCAST and recipients else None
    message = SwarmMessage(
        sender_id=sender_id, target_id=target, payload=payload, message_type=kind
    )
    return MessageRouted(message=message, recipients=tuple(r.id for r in recipients))


def runner_output(
    agent: AgentState, *, cached: bool = False, rate_limit: RateLimitInfo | None = None
) -> RunnerOutput:
    return RunnerOutput(
        agent_id=agent.id,
        content="x",
        tokens_used=5,
        latency_ms=180.0,
        model_used="stub-model",
        provider="vllm",
        cached_hit=cached,
        rate_limit=rate_limit,
    )


# --- Pure helpers ---------------------------------------------------------------------


class TestStyleHelpers:
    @pytest.mark.parametrize(
        ("seconds", "text"), [(-5, "0s"), (42.9, "42s"), (185, "3m 05s"), (3720, "1h 02m")]
    )
    def test_format_duration(self, seconds: float, text: str) -> None:
        assert format_duration(seconds) == text

    def test_format_cost(self) -> None:
        assert (format_cost(0.0015), format_cost(12.345)) == ("$0.0015", "$12.35")

    def test_one_line(self) -> None:
        assert one_line("a\n  b\tc", 10) == "a b c"
        assert one_line("abcdefghij", 5) == "abcd…"
        assert one_line("abc", 0) == "…"

    def test_badges_cover_every_status(self) -> None:
        assert {status_badge(s).plain.split()[1] for s in S} == {s.value.upper() for s in S}


class TestLabels:
    def test_agent_label_layout_and_markup_safety(self) -> None:
        state = AgentState(
            name="evil",
            role=AgentRole.EXECUTOR,
            status=S.RUNNING,
            current_task=EVIL + " do   the\nthing " * 20,
            total_tokens=12345,
        )
        label = agent_label(state, step=2)
        assert label.plain.startswith("2. ▶ RUNNING  evil (executor)  — [bold red]PWN")
        assert label.plain.endswith("12,345 tok")
        assert "\n" not in label.plain
        assert "…" in label.plain
        assert not any(
            "link" in str(span.style) or str(span.style) == "bold red" for span in label.spans
        )

    def test_idle_label_omits_task_and_tokens(self) -> None:
        assert agent_label(make_agent("a")).plain == "○ IDLE  a (executor)"

    def test_swarm_label_counts(self, mesh_topology: SwarmTopology) -> None:
        m1, m2, m3 = mesh_topology.agents
        states = {
            m1.id: transition(m1, S.RUNNING, task="t"),
            m2.id: transition(transition(m2, S.RUNNING, task="t"), S.ERROR),
            m3.id: m3,
        }
        plain = swarm_label(mesh_topology, states).plain
        assert plain.startswith("t  mesh  3 agents")
        assert "▶ 1" in plain
        assert "✖ 1" in plain
        assert "◷" not in plain

    @pytest.mark.parametrize(
        ("remaining", "style"), [(60, "green3"), (20, "yellow3"), (1, "red3"), (99, "green3")]
    )
    def test_headroom_bar(self, remaining: int, style: str) -> None:
        bar = headroom_bar(remaining, 60)
        assert bar.style == style
        assert len(bar.plain) == 10
        assert len(headroom_bar(0, 0).plain) == 10


class TestAlertClassification:
    @pytest.mark.parametrize(
        ("exc", "category", "severity"),
        [
            (
                GatewayRateLimitError("429", retry_after_s=7),
                AlertCategory.RATE_LIMIT,
                AlertSeverity.WARNING,
            ),
            (GatewayTimeoutError("t"), AlertCategory.TIMEOUT, AlertSeverity.WARNING),
            (GatewayConnectionError("c"), AlertCategory.CONNECTION, AlertSeverity.WARNING),
            (RoutingError("no"), AlertCategory.ROUTING, AlertSeverity.ERROR),
            (
                InvalidStateTransitionError(uuid4(), S.IDLE, S.WAITING),
                AlertCategory.STATE,
                AlertSeverity.ERROR,
            ),
            (GatewayClientError("x", retryable=True), AlertCategory.GATEWAY, AlertSeverity.WARNING),
            (KeyError("k"), AlertCategory.AGENT, AlertSeverity.ERROR),
        ],
    )
    def test_exceptions(
        self, exc: Exception, category: AlertCategory, severity: AlertSeverity
    ) -> None:
        alert = alert_from_exception(exc, agent_name="a")
        assert (alert.category, alert.severity, alert.agent_name) == (category, severity, "a")

    def test_rate_limit_carries_retry_after(self) -> None:
        assert (
            alert_from_exception(GatewayRateLimitError("429", retry_after_s=7)).retry_after_s == 7
        )

    def test_validation_error_is_summarised_on_one_line(self) -> None:
        with pytest.raises(ValidationError) as caught:
            SwarmMessage.model_validate({"sender_id": "x"})
        alert = alert_from_exception(caught.value)
        assert alert.category is AlertCategory.SCHEMA
        assert alert.message.startswith("SwarmMessage: 3 validation error(s); ")
        assert "(sender_id)" in alert.message
        assert "\n" not in alert.message

    def test_empty_messages(self) -> None:
        assert alert_from_exception(RuntimeError()).message == "RuntimeError"

    def test_failures(self) -> None:
        agent_id = uuid4()
        retryable = AgentFailed(
            agent_id=agent_id, error="x", error_type="GatewayConnectionError", retryable=True
        )
        assert alert_from_failure(retryable).severity is AlertSeverity.WARNING
        flagged = AgentFailed(agent_id=agent_id, error="x", error_type="KeyError", retryable=True)
        assert (alert_from_failure(flagged).category, alert_from_failure(flagged).severity) == (
            AlertCategory.AGENT,
            AlertSeverity.WARNING,
        )
        assert categorize("GatewayRateLimitError") is AlertCategory.RATE_LIMIT

    def test_alert_line(self) -> None:
        alert = Alert(
            severity=AlertSeverity.WARNING,
            category=AlertCategory.RATE_LIMIT,
            message=EVIL,
            agent_name="w1",
            retry_after_s=65,
            timestamp=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
        )
        plain = alert_line(alert).plain
        assert "⚠ RATE-LIMIT" in plain
        assert " w1: " in plain
        assert plain.endswith("(retry in 1m 05s)")
        assert EVIL in plain


# --- Mounted widgets ------------------------------------------------------------------------


class Harness(App[None]):
    """The four panels, fed by tests directly."""

    def __init__(self, topology: SwarmTopology) -> None:
        super().__init__()
        self.topology = topology
        self.selected: list[object] = []

    def compose(self) -> ComposeResult:
        yield AgentTree(self.topology)
        yield StreamingLog()
        yield TokenTracker()
        yield AlertsPanel()

    def on_mount(self) -> None:
        for widget in (
            self.query_one(StreamingLog),
            self.query_one(TokenTracker),
            self.query_one(AlertsPanel),
        ):
            widget.load_topology(self.topology)

    def on_agent_tree_agent_selected(self, message: AgentTree.AgentSelected) -> None:
        self.selected.append(message.agent_id)


class TestAgentTree:
    @pytest.mark.parametrize("strategy", list(RoutingStrategy))
    async def test_layout_per_strategy(self, strategy: RoutingStrategy) -> None:
        boss = make_agent("boss", AgentRole.ORCHESTRATOR)
        topology = make_topology(strategy, make_agent("a"), boss, make_agent("b"))
        tree = AgentTree(topology)
        root = tree.root
        if strategy is RoutingStrategy.HIERARCHICAL:
            assert [n.data for n in root.children] == [boss.id]
            assert len(root.children[0].children) == 2
        else:
            assert len(root.children) == 3
        numbered = [str(n.label)[:2] for n in root.children]
        assert (numbered == ["1.", "2.", "3."]) is (strategy is RoutingStrategy.SEQUENTIAL)
        assert tree.states == topology.agents

    async def test_updates_selection_and_no_collapse(
        self, sequential_topology: SwarmTopology
    ) -> None:
        app = Harness(sequential_topology)
        async with app.run_test() as pilot:
            tree = app.query_one(AgentTree)
            p1 = sequential_topology.agents[0]
            busy = transition(p1, S.RUNNING, task="Survey")
            tree.apply_event(AgentUpdated(agent=busy))
            assert "RUNNING" in str(tree.root.children[0].label)
            assert "▶ 1" in str(tree.root.label)
            tree.update_agent(make_agent("stranger"))  # ignored: not in the topology
            tree.apply_event(TaskCompleted(agent_id=p1.id, output="x"))  # ignored event kind
            tree.focus()
            tree.move_cursor(tree.root.children[1])
            await pilot.press("enter")
            tree.move_cursor(tree.root)
            await pilot.press("enter")
            await pilot.pause()
            assert app.selected == [sequential_topology.agents[1].id, None]
            assert tree.root.is_expanded  # auto_expand disabled

    def test_empty_tree(self) -> None:
        tree = AgentTree()
        tree.update_agent(make_agent("x"))  # no topology loaded: ignored
        assert tree.states == ()


class TestStreamingLog:
    async def test_turns_messages_results_and_failures(
        self, hierarchical_topology: SwarmTopology
    ) -> None:
        w1, boss, w2 = hierarchical_topology.agents
        app = Harness(hierarchical_topology)
        async with app.run_test(size=(140, 40)) as pilot:
            log = app.query_one(StreamingLog)
            history = log.query_one(RichLog)
            live = log.query_one("#live", Static)
            log.apply_event(
                routed(hierarchical_topology, MessageType.SYSTEM, boss, payload="Build it")
            )
            log.append_delta(boss.id, "")  # empty deltas are ignored
            log.append_delta(boss.id, "plan ")
            log.append_delta(boss.id, EVIL)
            await pilot.pause(0.15)
            assert live.display
            log.apply_event(routed(boss, MessageType.BROADCAST, w1, w2, payload="sub-task"))
            log.append_delta(w1.id, "w1 out")
            log.apply_event(
                AgentUpdated(agent=transition(transition(w1, S.RUNNING, task="t"), S.IDLE))
            )
            log.append_delta(w2.id, "partial")
            log.apply_event(AgentFailed(agent_id=w2.id, error="boom", error_type="KeyError"))
            log.apply_event(TaskCompleted(agent_id=boss.id, output="final answer"))
            log.end_turn(uuid4())  # nothing in flight: no-op
            await pilot.pause(0.15)
            text = lines(history)
            assert "⇒ task → boss  Build it" in text
            assert "boss (orchestrator)" in text
            assert EVIL in text
            assert "✉ boss → all  sub-task" in text
            assert "w1 (executor)" in text
            assert "w1 out" in text
            assert "w2 failed: boom" in text
            assert "partial" in text
            assert "✔ result from boss" in text
            assert "final answer" in text
            assert not live.display

    async def test_task_sent_to_every_agent_lists_all_recipients(
        self, mesh_topology: SwarmTopology
    ) -> None:
        app = Harness(mesh_topology)
        async with app.run_test(size=(140, 40)) as pilot:
            log = app.query_one(StreamingLog)
            to_all = SwarmMessage(
                sender_id=mesh_topology.swarm_id,
                payload="all hands",
                message_type=MessageType.SYSTEM,
            )
            recipients = tuple(a.id for a in mesh_topology.agents)
            log.apply_event(MessageRouted(message=to_all, recipients=recipients))
            await pilot.pause()
            assert "\u21d2 task \u2192 m1, m2, m3  all hands" in lines(log.query_one(RichLog))

    async def test_duplicate_result_is_compact_and_follows_its_turn(
        self, mesh_topology: SwarmTopology
    ) -> None:
        m1 = mesh_topology.agents[0]
        app = Harness(mesh_topology)
        async with app.run_test(size=(140, 40)) as pilot:
            log = app.query_one(StreamingLog)
            log.append_delta(m1.id, "same text")
            log.apply_event(TaskCompleted(agent_id=m1.id, output="same text"))  # before IDLE
            await pilot.pause()
            rows = lines(log.query_one(RichLog)).splitlines()
            rule = next(i for i, row in enumerate(rows) if row.startswith("m1 (executor)"))
            result = next(i for i, row in enumerate(rows) if "result from m1" in row)
            assert rule < result
            assert sum("same text" in row for row in rows) == 1

    async def test_focus_filter_scroll_lock_and_clear(self, mesh_topology: SwarmTopology) -> None:
        m1, m2, m3 = mesh_topology.agents
        app = Harness(mesh_topology)
        async with app.run_test(size=(140, 40)) as pilot:
            log = app.query_one(StreamingLog)
            for agent in (m1, m2):
                log.append_delta(agent.id, f"{agent.name} thought")
                log.end_turn(agent.id)
            log.append_delta(m3.id, "m3 live")
            log.focus_agent(m1.id)
            await pilot.pause(0.15)
            filtered = lines(log.query_one(RichLog))
            assert "m1 thought" in filtered
            assert "m2 thought" not in filtered
            assert not log.query_one("#live", Static).display  # m3's live turn is filtered out too
            assert "m1" in str(log.border_title)
            assert log.focused_agent == m1.id
            log.focus_agent(None)
            assert "m2 thought" in lines(log.query_one(RichLog))
            log.query_one(RichLog).focus()
            await pilot.press("s")
            assert not follows(log)
            assert not log.query_one(RichLog).auto_scroll
            assert "scroll locked" in str(log.border_title)
            await pilot.press("s")
            assert follows(log)
            log.action_clear_log()
            assert not lines(log.query_one(RichLog)).strip()

    async def test_live_area_keeps_the_tail_of_long_output(
        self, mesh_topology: SwarmTopology
    ) -> None:
        m1 = mesh_topology.agents[0]
        app = Harness(mesh_topology)
        async with app.run_test(size=(140, 40)) as pilot:
            log = app.query_one(StreamingLog)
            log.append_delta(m1.id, "HEAD" + "a" * MAX_LIVE_CHARS + "TAIL")
            await pilot.pause(0.15)
            console = Console(width=120, record=True, file=io.StringIO())
            console.print(log.query_one("#live", Static).content)
            shown = console.export_text().replace("\n", "")
            assert "m1  streaming" in shown
            assert "\u2026a" in shown
            assert shown.endswith("TAIL")
            assert "HEAD" not in shown
            assert shown.count("a") <= MAX_LIVE_CHARS + 2

    def test_entries_before_mount_and_unknown_names(self) -> None:
        log = StreamingLog()
        stranger = make_agent("stranger")
        log.append_delta(stranger.id, "x")
        log.end_turn(stranger.id)  # stored while unmounted
        log.focus_agent(None)
        assert log._agent_name(stranger.id) == str(stranger.id)[:8]
        assert log._agent_style(stranger.id) == "bold"


class TestTokenTracker:
    async def test_metrics_calls_and_rate_limit(
        self, mesh_topology: SwarmTopology, future_reset: datetime
    ) -> None:
        app = Harness(mesh_topology)
        async with app.run_test(size=(140, 40)) as pilot:
            tracker = app.query_one(TokenTracker)
            assert tracker.total_agents == 3
            assert [label for label, _ in tracker.rate_limit_rows()] == ["Rate limit"]
            m1 = mesh_topology.agents[0]
            info = RateLimitInfo(limit=60, remaining=9, reset_at=future_reset)
            tracker.record_output(runner_output(m1, cached=True, rate_limit=info))
            tracker.record_output(runner_output(m1))
            assert (tracker.gateway_calls, tracker.cache_hits) == (2, 1)
            assert tracker.rate_limit == info
            rows = {label: value.plain for label, value in tracker.rate_limit_rows()}
            assert rows["Rate limit"].endswith(" 9/60")
            assert rows["Resets in"].endswith("s")
            tracker.update_rate_limit(
                info.model_copy(update={"remaining": 0, "retry_after_s": 7.0})
            )
            assert dict(tracker.rate_limit_rows())["Retry in"].plain == "7s"
            past = datetime.now(UTC) - timedelta(seconds=1)
            tracker.update_rate_limit(RateLimitInfo(limit=60, remaining=5, reset_at=past))
            assert dict(tracker.rate_limit_rows())["Window"].plain == "reset"
            tracker._tick()
            await pilot.pause()
            assert tracker.region.height >= 9  # the panel grew to fit every row

    def test_injected_clock_drives_the_countdown(self) -> None:
        now = datetime(2030, 1, 1, tzinfo=UTC)
        tracker = TokenTracker(clock=lambda: now)
        tracker.update_rate_limit(
            RateLimitInfo(limit=10, remaining=5, reset_at=now + timedelta(seconds=90))
        )
        assert dict(tracker.rate_limit_rows())["Resets in"].plain == "1m 30s"
        tracker._tick()  # not mounted: harmless
        assert isinstance(tracker.render(), object)


class TestAlertsPanel:
    async def test_events_exceptions_counts_and_clear(self, mesh_topology: SwarmTopology) -> None:
        m1 = mesh_topology.agents[0]
        app = Harness(mesh_topology)
        async with app.run_test(size=(140, 40)) as pilot:
            panel = app.query_one(AlertsPanel)
            panel.apply_event(
                AgentFailed(
                    agent_id=m1.id, error="slow", error_type="GatewayTimeoutError", retryable=True
                )
            )
            panel.apply_event(TaskCompleted(agent_id=m1.id, output="x"))  # not an alert
            alert = panel.notify_exception(RoutingError("nope"), agent_id=m1.id)
            panel.notify_exception(ValueError("bad"))
            panel.info("hello")
            await pilot.pause()
            assert alert.agent_name == "m1"
            text = lines(panel.query_one(RichLog))
            assert "TIMEOUT" in text
            assert "m1:" in text
            assert "ROUTING" in text
            assert "SYSTEM" in text
            assert (panel.count(AlertSeverity.WARNING), panel.count(AlertSeverity.ERROR)) == (1, 2)
            assert str(panel.border_subtitle) == "⚠ 1  ✖ 2"
            panel.clear_alerts()
            assert panel.alerts == ()
            assert not panel.border_subtitle

    async def test_alerts_added_before_mount_are_replayed(
        self, mesh_topology: SwarmTopology
    ) -> None:
        panel = AlertsPanel()
        panel.info("early")

        class Solo(App[None]):
            def compose(self) -> ComposeResult:
                yield panel

        async with Solo().run_test() as pilot:
            await pilot.pause()
            assert "early" in lines(panel.query_one(RichLog))

    def test_clear_while_unmounted(self) -> None:
        panel = AlertsPanel()
        panel.info("x")
        panel.clear_alerts()
        assert panel.alerts == ()
        assert panel.count(AlertSeverity.INFO) == 0

    def test_alert_model_bounds(self) -> None:
        with pytest.raises(ValidationError):
            Alert(severity=AlertSeverity.INFO, category=AlertCategory.SYSTEM, message="")
        assert isinstance(
            alert_line(
                Alert(severity=AlertSeverity.INFO, category=AlertCategory.SYSTEM, message="m")
            ),
            Text,
        )
