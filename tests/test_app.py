"""Full SwarmApp flows under Textual's Pilot: wiring, keys, dialog, layout, shutdown, exit codes."""

import asyncio
from uuid import uuid4

import pytest
from textual.pilot import Pilot
from textual.widgets import Input, RichLog, Select

from agent_swarm_tui import SwarmApp
from agent_swarm_tui.client import delegating_reply, next_hop_reply
from agent_swarm_tui.core import AgentReply, SwarmError
from agent_swarm_tui.presets import build_topology, reply_factory_for
from agent_swarm_tui.schemas import (
    AgentRole,
    AgentState,
    AgentStatus,
    RoutingStrategy,
    SwarmMessage,
)
from agent_swarm_tui.widgets import (
    AgentTree,
    AlertsPanel,
    DispatchDialog,
    StreamingLog,
    TokenTracker,
)
from tests.conftest import MockGateway, Reply, good_stream, rate_limit_headers

S = AgentStatus


def history(app: SwarmApp) -> str:
    log = app.query_one(StreamingLog).query_one(RichLog)
    return "\n".join("".join(segment.text for segment in strip) for strip in log.lines)


def dialog_open(app: SwarmApp) -> bool:
    return isinstance(app.screen, DispatchDialog)


def follows(log: StreamingLog) -> bool:
    return log.auto_scroll


def swarm_tasks_left() -> list[str]:
    current = asyncio.current_task()
    return [
        t.get_name()
        for t in asyncio.all_tasks()
        if t is not current and not t.done() and t.get_name().startswith(("swarm:", "agent:"))
    ]


async def until(pilot: Pilot[None], condition: object, within: float = 5.0) -> bool:
    assert callable(condition)
    deadline = asyncio.get_running_loop().time() + within
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            return False
        await pilot.pause(0.02)
    return True


class TestPresets:
    @pytest.mark.parametrize("strategy", list(RoutingStrategy))
    def test_four_agent_swarms(self, strategy: RoutingStrategy) -> None:
        topology = build_topology(strategy, name="demo")
        assert (topology.name, topology.routing_strategy, len(topology.agents)) == (
            "demo",
            strategy,
            4,
        )
        head = topology.agents[0]
        expected = (
            ("lead", AgentRole.ORCHESTRATOR)
            if strategy is RoutingStrategy.HIERARCHICAL
            else ("planner", AgentRole.PLANNER)
        )
        assert (head.name, head.role) == expected

    def test_reply_factories(self) -> None:
        assert reply_factory_for(RoutingStrategy.HIERARCHICAL) is delegating_reply
        assert reply_factory_for(RoutingStrategy.SEQUENTIAL) is next_hop_reply
        assert reply_factory_for(RoutingStrategy.MESH) is next_hop_reply


class TestFullFlow:
    async def test_pipeline_wiring_and_graceful_quit(self, gateway: MockGateway) -> None:
        gateway.responder = lambda body: Reply(
            chunks=good_stream(
                f"{body['prompt'].split('You are ', 1)[1].split(',', 1)[0]} ", "done"
            ),
            headers=rate_limit_headers(50),
        )
        client = gateway.client()
        topology = build_topology(RoutingStrategy.SEQUENTIAL, name="demo")
        app = SwarmApp(
            topology, client=client, initial_task="Plan a picnic", cost_per_million_tokens=2.0
        )
        async with app.run_test(size=(150, 44)) as pilot:
            assert await until(pilot, lambda: len(app.swarm.results) == 1)
            await pilot.pause(0.1)
            assert app.sub_title == "demo · sequential · 4 agents · running"
            assert app.swarm.results[0].output == "reviewer done"
            text = history(app)
            assert "⇒ task → planner  Plan a picnic" in text
            assert "✉ planner → researcher" in text
            assert "reviewer (reviewer)" in text
            tracker = app.query_one(TokenTracker)
            assert (
                tracker.metrics.total_tokens,
                tracker.metrics.messages_processed,
                tracker.gateway_calls,
            ) == (40, 4, 4)
            await pilot.pause(1.1)  # telemetry tick pulls client.last_rate_limit
            assert tracker.rate_limit is not None
            assert tracker.rate_limit.remaining == 50
            tree = app.query_one(AgentTree)
            assert all("IDLE" in str(node.label) for node in tree._agent_nodes.values())
            alerts = app.query_one(AlertsPanel)
            assert any("started via http://127.0.0.1:8000/" in a.message for a in alerts.alerts)

            await pilot.press("q")
            assert app.check_action("dispatch", ()) is None  # disabled while draining
            assert await until(pilot, lambda: app.return_code is not None)
            assert app.swarm_status == "stopped"
        assert app.return_code == 0
        assert all(a.status is S.TERMINATED for a in app.swarm.agents)
        assert client._http.is_closed
        assert not swarm_tasks_left()

    async def test_tree_selection_focuses_the_stream(self, gateway: MockGateway) -> None:
        app = SwarmApp(build_topology(RoutingStrategy.MESH), client=gateway.client())
        async with app.run_test(size=(150, 44)) as pilot:
            tree = app.query_one(AgentTree)
            tree.focus()
            tree.move_cursor(tree.root.children[1])
            await pilot.press("enter")
            await pilot.pause()
            assert app.query_one(StreamingLog).focused_agent == app.swarm.agents[1].id
            tree.move_cursor(tree.root)
            await pilot.press("enter")
            await pilot.pause()
            assert app.query_one(StreamingLog).focused_agent is None
            assert tree.root.is_expanded


class TestKeys:
    async def test_scroll_lock_and_clear(self, gateway: MockGateway) -> None:
        app = SwarmApp(
            build_topology(RoutingStrategy.MESH), client=gateway.client(), initial_task="x"
        )
        async with app.run_test(size=(150, 44)) as pilot:
            await until(pilot, lambda: len(app.swarm.results) == 1)
            log, alerts = app.query_one(StreamingLog), app.query_one(AlertsPanel)
            await pilot.press("s")
            assert not follows(log)
            await pilot.press("s")
            assert follows(log)
            await pilot.pause()
            assert history(app).strip()
            assert alerts.alerts
            await pilot.press("c")
            await pilot.pause()
            assert not history(app).strip()
            assert not alerts.alerts

    async def test_dispatch_dialog(self, gateway: MockGateway) -> None:
        topology = build_topology(RoutingStrategy.SEQUENTIAL)
        app = SwarmApp(topology, client=gateway.client())
        async with app.run_test(size=(150, 44)) as pilot:
            await until(pilot, lambda: app.swarm_status == "running")
            await pilot.press("d")
            await pilot.pause()
            assert dialog_open(app)
            assert app.check_action("toggle_scroll", ()) is False  # blocked behind the modal
            await pilot.press("escape")
            await pilot.pause()
            assert not dialog_open(app)
            assert not gateway.requests

            await pilot.press("d")
            await pilot.pause()
            await pilot.click("#cancel")
            await pilot.pause()
            assert not dialog_open(app)

            await pilot.press("d")
            await pilot.pause()
            await pilot.press("enter")  # empty task
            await pilot.pause()
            assert dialog_open(app)
            assert app.screen.query_one("#error").display
            await pilot.press(*"scdq build")  # typed into the input, not bindings
            await pilot.pause()
            assert app.screen.query_one("#task", Input).value == "scdq build"
            assert app.query_one(StreamingLog).auto_scroll
            assert app.swarm_status == "running"
            app.screen.query_one("#target", Select).value = str(topology.agents[2].id)  # builder
            await pilot.click("#dispatch")
            assert await until(pilot, lambda: len(app.swarm.results) == 1)
            assert [b["prompt"].split(",", 1)[0] for b in gateway.bodies] == [
                "You are builder",
                "You are reviewer",
            ]

    async def test_rejected_dispatch_raises_an_alert(self, gateway: MockGateway) -> None:
        app = SwarmApp(build_topology(RoutingStrategy.MESH), client=gateway.client())
        async with app.run_test(size=(150, 44)) as pilot:
            await until(pilot, lambda: app.swarm_status == "running")
            app._dispatch("x", uuid4())
            app._dispatch("", None)
            await pilot.pause()
            categories = [a.category.value for a in app.query_one(AlertsPanel).alerts]
            assert categories[-2:] == ["routing", "schema"]
            assert app.swarm_status == "running"


class TestLayout:
    @pytest.mark.parametrize(
        ("width", "screen_class", "left_width", "stacked"),
        [(100, "-narrow", 100, True), (150, "-normal", 64, False), (190, "-wide", 76, False)],
    )
    async def test_breakpoints(
        self, gateway: MockGateway, width: int, screen_class: str, left_width: int, stacked: bool
    ) -> None:
        app = SwarmApp(build_topology(RoutingStrategy.MESH), client=gateway.client())
        async with app.run_test(size=(width, 44)) as pilot:
            await pilot.pause()
            left, right = app.query_one("#left"), app.query_one("#right")
            assert app.screen.has_class(screen_class)
            assert left.region.width == left_width
            assert (right.region.y > left.region.y) is stacked


class TestShutdown:
    async def test_q_twice_cancels_in_flight_work(self, gateway: MockGateway) -> None:
        gateway.responder = lambda body: Reply(chunks=good_stream("x"), ttft=30)
        client = gateway.client()
        app = SwarmApp(
            build_topology(RoutingStrategy.MESH),
            client=client,
            initial_task="long",
            drain_timeout=60,
        )
        async with app.run_test(size=(150, 40)) as pilot:
            assert await until(pilot, lambda: app.swarm.metrics.active_agents == 1)
            await pilot.press("q")
            await pilot.pause(0.1)
            assert app.swarm_status == "draining"
            assert app.is_running
            await pilot.press("q")
            assert await until(pilot, lambda: app.return_code is not None)
        assert gateway.cancelled == 1
        assert app.return_code == 0
        assert client._http.is_closed
        assert not swarm_tasks_left()

    async def test_third_q_exits_immediately(self, gateway: MockGateway) -> None:
        gateway.responder = lambda body: Reply(chunks=good_stream("x"), ttft=30)
        app = SwarmApp(
            build_topology(RoutingStrategy.MESH), client=gateway.client(), initial_task="long"
        )
        async with app.run_test(size=(150, 40)) as pilot:
            await until(pilot, lambda: app.swarm.metrics.active_agents == 1)
            app.swarm.abort_drain = lambda: None  # type: ignore[method-assign] # make the 2nd q ineffective
            await pilot.press("q", "q", "q")
            assert await until(pilot, lambda: app.return_code is not None)
        assert not swarm_tasks_left()

    async def test_abrupt_exit_hard_stops_the_swarm(self, gateway: MockGateway) -> None:
        gateway.responder = lambda body: Reply(chunks=good_stream("x"), ttft=30)
        client = gateway.client()
        app = SwarmApp(
            build_topology(RoutingStrategy.HIERARCHICAL), client=client, initial_task="x"
        )
        async with app.run_test(size=(150, 40)) as pilot:
            await until(pilot, lambda: app.swarm.metrics.active_agents == 1)
            app.exit()
            await pilot.pause(0.2)
        assert gateway.cancelled == 1
        assert all(a.status is S.TERMINATED for a in app.swarm.agents)
        assert client._http.is_closed
        assert not swarm_tasks_left()

    async def test_q_while_dialog_open_closes_it_and_quits(self, gateway: MockGateway) -> None:
        app = SwarmApp(build_topology(RoutingStrategy.MESH), client=gateway.client())
        async with app.run_test(size=(150, 40)) as pilot:
            await until(pilot, lambda: app.swarm_status == "running")
            await pilot.press("d")
            await pilot.pause()
            await app.action_quit()
            assert await until(pilot, lambda: app.return_code is not None)
        assert app.return_code == 0


class BrokenRunner:
    """Returns ``None`` instead of an ``AgentReply``: a bug that crashes the swarm."""

    async def run(self, agent: AgentState, message: SwarmMessage) -> AgentReply:
        return None  # type: ignore[return-value]


class EchoRunner:
    async def run(self, agent: AgentState, message: SwarmMessage) -> AgentReply:
        return AgentReply(output="done", forward_to=())


class TestFailures:
    async def test_crashed_swarm_exits_with_code_1(self) -> None:
        app = SwarmApp(
            build_topology(RoutingStrategy.MESH), runner=BrokenRunner(), initial_task="boom"
        )
        async with app.run_test(size=(150, 40)) as pilot:
            assert await until(pilot, lambda: app.swarm_status == "crashed", within=3)
            assert app.check_action("dispatch", ()) is None
            await pilot.press("q")
            assert await until(pilot, lambda: app.return_code is not None)
        assert app.return_code == 1
        assert not swarm_tasks_left()

    async def test_crash_then_abrupt_exit_is_contained(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        app = SwarmApp(
            build_topology(RoutingStrategy.MESH), runner=BrokenRunner(), initial_task="boom"
        )
        async with app.run_test(size=(150, 40)) as pilot:
            await until(pilot, lambda: app.swarm_status == "crashed", within=3)
            app.exit()
            await pilot.pause()
        assert "swarm crashed during shutdown" in caplog.text

    async def test_start_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        app = SwarmApp(build_topology(RoutingStrategy.MESH), runner=EchoRunner())

        async def refuse() -> None:
            raise SwarmError("cannot start")

        monkeypatch.setattr(app.swarm, "start_swarm", refuse)
        async with app.run_test(size=(150, 40)) as pilot:
            assert await until(pilot, lambda: app.swarm_status == "crashed")
            assert any("cannot start" in a.message for a in app.query_one(AlertsPanel).alerts)
            await pilot.press("q")  # lifecycle already over: exits at once
            assert await until(pilot, lambda: app.return_code is not None)
        assert app.return_code == 1

    async def test_gateway_429_reaches_alerts_and_telemetry(self, gateway: MockGateway) -> None:
        gateway.responder = lambda body: Reply(
            status=429,
            json_body={
                "error": {
                    "code": "rate_limited",
                    "message": "Rate limit exceeded.",
                    "retryable": True,
                }
            },
            headers={**rate_limit_headers(0), "retry-after": "9"},
        )
        app = SwarmApp(
            build_topology(RoutingStrategy.SEQUENTIAL), client=gateway.client(), initial_task="go"
        )
        async with app.run_test(size=(150, 40)) as pilot:
            alerts, tracker = app.query_one(AlertsPanel), app.query_one(TokenTracker)
            assert await until(
                pilot, lambda: any(a.category.value == "rate-limit" for a in alerts.alerts)
            )
            await pilot.pause(1.1)
            limited = next(a for a in alerts.alerts if a.category.value == "rate-limit")
            assert (limited.severity.value, limited.agent_name) == ("warning", "planner")
            assert tracker.rate_limit is not None
            assert tracker.rate_limit.remaining == 0
            assert app.swarm.agent(app.swarm.entry_point).status is S.ERROR
            assert app.swarm.is_running

    async def test_custom_runner_without_client(self) -> None:
        app = SwarmApp(build_topology(RoutingStrategy.MESH), runner=EchoRunner(), initial_task="x")
        async with app.run_test(size=(150, 40)) as pilot:
            assert await until(pilot, lambda: len(app.swarm.results) == 1)
            alerts = app.query_one(AlertsPanel).alerts
            assert alerts[0].message == "swarm 'swarm' started"  # no "via <url>" without a client
            await pilot.press("q")
            assert await until(pilot, lambda: app.return_code is not None)
        assert app.return_code == 0
