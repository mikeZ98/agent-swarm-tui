"""Swarm lifecycle, concurrency, failure containment, shutdown and metrics."""

import asyncio
import logging
import math
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from agent_swarm_tui.core import (
    AgentFailed,
    AgentNotFoundError,
    AgentReply,
    AgentUpdated,
    InvalidStateTransitionError,
    MessageRouted,
    StatusHook,
    SwarmError,
    SwarmOrchestrator,
    TaskCompleted,
    transition,
)
from agent_swarm_tui.schemas import (
    MAX_TASK_CHARS,
    AgentRole,
    AgentState,
    AgentStatus,
    MessageType,
    RoutingStrategy,
    SwarmMessage,
    SwarmTopology,
)
from tests.conftest import EventCollector, ScriptedRunner, make_agent, make_topology, wait_for

S = AgentStatus


def leftover_swarm_tasks() -> list[str]:
    current = asyncio.current_task()
    return [
        t.get_name()
        for t in asyncio.all_tasks()
        if t is not current and not t.done() and t.get_name().startswith(("swarm:", "agent:"))
    ]


async def append_name(agent: AgentState, message: SwarmMessage) -> AgentReply:
    return AgentReply(tokens_used=10, output=f"{message.payload}>{agent.name}")


async def final(agent: AgentState, message: SwarmMessage) -> AgentReply:
    return AgentReply(output=f"ok:{agent.name}", forward_to=())


class TestLifecycle:
    async def test_sequential_pipeline_end_to_end(
        self, sequential_topology: SwarmTopology, events: EventCollector
    ) -> None:
        swarm = SwarmOrchestrator(
            sequential_topology,
            ScriptedRunner(append_name),
            listener=events,
            cost_per_million_tokens=2.0,
        )
        async with swarm:
            dispatched = swarm.dispatch_task("in")
            assert dispatched.message_type is MessageType.SYSTEM
            assert dispatched.target_id == swarm.entry_point == sequential_topology.agents[0].id
            assert await swarm.stop_swarm() is True
        assert [r.output for r in swarm.results] == ["in>p1>p2>p3"]
        metrics = swarm.metrics
        assert (metrics.messages_processed, metrics.total_tokens, metrics.active_agents) == (
            3,
            30,
            0,
        )
        assert math.isclose(metrics.estimated_cost_usd, 30 * 2.0 / 1_000_000)
        assert all(a.status is S.TERMINATED for a in swarm.agents)
        assert [len(e.recipients) for e in events.of(MessageRouted)] == [1, 1, 1]
        assert len(events.of(TaskCompleted)) == 1
        assert events.of(AgentUpdated)[-1].agent.status is S.TERMINATED
        assert swarm.topology is sequential_topology
        assert not leftover_swarm_tasks()

    async def test_hierarchical_delegation_drains_completely(
        self, hierarchical_topology: SwarmTopology
    ) -> None:
        reports: list[str] = []

        async def hub(agent: AgentState, message: SwarmMessage) -> AgentReply:
            if agent.role is AgentRole.ORCHESTRATOR:
                if message.message_type is MessageType.SYSTEM:
                    return AgentReply(output="sub-task", broadcast=True)
                reports.append(message.payload)
                if len(reports) == 2:
                    return AgentReply(output="|".join(sorted(reports)), forward_to=())
                return AgentReply()
            return AgentReply(tokens_used=1, output=f"done:{agent.name}")

        async with SwarmOrchestrator(hierarchical_topology, ScriptedRunner(hub)) as swarm:
            swarm.dispatch_task("plan")
            assert await swarm.stop_swarm()  # drain follows the whole forwarded chain
        assert [r.output for r in swarm.results] == ["done:w1|done:w2"]

    async def test_agents_run_concurrently(self, mesh_topology: SwarmTopology) -> None:
        barrier = asyncio.Barrier(3)  # only releases if all three are inside run() at once

        async def meet(agent: AgentState, message: SwarmMessage) -> AgentReply:
            await asyncio.wait_for(barrier.wait(), timeout=5)
            return AgentReply(output="met", forward_to=())

        events = EventCollector()
        async with SwarmOrchestrator(mesh_topology, ScriptedRunner(meet), listener=events) as swarm:
            for agent in mesh_topology.agents:
                swarm.dispatch_task("go", agent_id=agent.id)
            assert await swarm.stop_swarm()
        # Run one at a time, the barrier would time out and every agent would fail.
        assert not events.of(AgentFailed)
        assert [r.output for r in swarm.results] == ["met"] * 3

    async def test_start_and_stop_guards(self, mesh_topology: SwarmTopology) -> None:
        swarm = SwarmOrchestrator(mesh_topology, ScriptedRunner(final))
        assert not swarm.is_running
        with pytest.raises(SwarmError, match="not running"):
            swarm.dispatch_task("x")
        await swarm.start_swarm()
        with pytest.raises(SwarmError, match="only be started once"):
            await swarm.start_swarm()
        assert await swarm.stop_swarm() is True
        assert await swarm.stop_swarm() is True  # idempotent
        with pytest.raises(SwarmError, match="not running"):
            swarm.dispatch_task("x")

    async def test_stop_before_start_is_a_noop(self, mesh_topology: SwarmTopology) -> None:
        swarm = SwarmOrchestrator(mesh_topology, ScriptedRunner(final))
        assert await swarm.stop_swarm() is True
        with pytest.raises(SwarmError):
            await swarm.start_swarm()


class TestDispatch:
    async def test_unknown_agent_and_invalid_task(self, mesh_topology: SwarmTopology) -> None:
        async with SwarmOrchestrator(mesh_topology, ScriptedRunner(final)) as swarm:
            with pytest.raises(AgentNotFoundError):
                swarm.dispatch_task("x", agent_id=uuid4())
            with pytest.raises(ValidationError):
                swarm.dispatch_task("")
            with pytest.raises(AgentNotFoundError):
                swarm.agent(uuid4())

    async def test_full_mailbox_rejects_dispatch(self, mesh_topology: SwarmTopology) -> None:
        gate = asyncio.Event()

        async def blocked(agent: AgentState, message: SwarmMessage) -> AgentReply:
            await gate.wait()
            return AgentReply()

        swarm = SwarmOrchestrator(mesh_topology, ScriptedRunner(blocked), mailbox_size=1)
        await swarm.start_swarm()
        target = mesh_topology.agents[0].id
        swarm.dispatch_task("1", agent_id=target)
        await wait_for(lambda: swarm.agent(target).status is S.RUNNING)
        swarm.dispatch_task("2", agent_id=target)  # fills the mailbox
        with pytest.raises(SwarmError, match="is full"):
            swarm.dispatch_task("3", agent_id=target)
        gate.set()
        assert await swarm.stop_swarm()

    async def test_long_payload_becomes_truncated_task_label(
        self, mesh_topology: SwarmTopology
    ) -> None:
        seen: list[str] = []

        async def capture(agent: AgentState, message: SwarmMessage) -> AgentReply:
            seen.append(agent.current_task or "")
            return AgentReply()

        async with SwarmOrchestrator(mesh_topology, ScriptedRunner(capture)) as swarm:
            swarm.dispatch_task("y" * (MAX_TASK_CHARS * 2))
            await swarm.stop_swarm()
        assert len(seen[0]) == MAX_TASK_CHARS
        assert seen[0].endswith("...")

    async def test_terminated_agent_drops_messages(self) -> None:
        dead = transition(make_agent("dead"), S.TERMINATED)
        topology = make_topology(RoutingStrategy.MESH, dead, make_agent("alive"))
        runner = ScriptedRunner(final)
        async with SwarmOrchestrator(topology, runner) as swarm:
            swarm.dispatch_task("x", agent_id=dead.id)
            assert await swarm.stop_swarm()
        assert runner.calls == []
        assert swarm.metrics.messages_processed == 0


class TestForwarding:
    async def test_broadcast_reaches_every_peer(self, mesh_topology: SwarmTopology) -> None:
        async def shout(agent: AgentState, message: SwarmMessage) -> AgentReply:
            if message.message_type is MessageType.SYSTEM:
                return AgentReply(output="hello all", broadcast=True)
            return AgentReply(output=f"{agent.name} heard", forward_to=())

        async with SwarmOrchestrator(mesh_topology, ScriptedRunner(shout)) as swarm:
            swarm.dispatch_task("start", agent_id=mesh_topology.agents[0].id)
            assert await swarm.stop_swarm()
        assert sorted(r.output for r in swarm.results) == ["m2 heard", "m3 heard"]

    async def test_broadcast_to_nobody_does_not_stall_the_drain(
        self, events: EventCollector
    ) -> None:
        async def lonely(agent: AgentState, message: SwarmMessage) -> AgentReply:
            return AgentReply(output="anyone?", broadcast=True)

        solo = make_topology(RoutingStrategy.MESH, make_agent("solo"))
        async with SwarmOrchestrator(solo, ScriptedRunner(lonely), listener=events) as swarm:
            swarm.dispatch_task("x")
            # A zero-recipient delivery must not count as in-flight work.
            assert await asyncio.wait_for(swarm.stop_swarm(drain_timeout=5), timeout=2) is True
        last = events.of(MessageRouted)[-1]
        assert (last.message.message_type, last.recipients) == (MessageType.BROADCAST, ())

    async def test_explicit_targets_and_silence(self, mesh_topology: SwarmTopology) -> None:
        m1, m2, m3 = (a.id for a in mesh_topology.agents)

        async def fan(agent: AgentState, message: SwarmMessage) -> AgentReply:
            if agent.id == m1:
                return AgentReply(output="to both", forward_to=(m2, m3))
            return AgentReply()  # no output: nothing forwarded, no result

        runner = ScriptedRunner(fan)
        async with SwarmOrchestrator(mesh_topology, runner) as swarm:
            swarm.dispatch_task("go", agent_id=m1)
            assert await swarm.stop_swarm()
        assert sorted(name for name, _ in runner.calls) == ["m1", "m2", "m3"]
        assert swarm.results == ()

    @pytest.mark.parametrize("target", ["self", "forbidden"])
    async def test_forwarding_violation_marks_agent_error(
        self, hierarchical_topology: SwarmTopology, events: EventCollector, target: str
    ) -> None:
        w1, _, w2 = (a.id for a in hierarchical_topology.agents)
        bad = w1 if target == "self" else w2

        async def rogue(agent: AgentState, message: SwarmMessage) -> AgentReply:
            return AgentReply(output="x", forward_to=(bad,))

        async with SwarmOrchestrator(
            hierarchical_topology, ScriptedRunner(rogue), listener=events
        ) as swarm:
            swarm.dispatch_task("t", agent_id=w1)
            assert await wait_for(lambda: swarm.agent(w1).status is S.ERROR)
            assert swarm.is_running
        failure = events.of(AgentFailed)[0]
        assert failure.error_type == "RoutingError"
        assert failure.retryable is False


class TestFailures:
    async def test_runner_error_is_contained_then_recovered(self, events: EventCollector) -> None:
        class FlakyError(Exception):
            retryable = True

        attempts = 0

        async def flaky(agent: AgentState, message: SwarmMessage) -> AgentReply:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise FlakyError("boom")
            return AgentReply(output="fine", forward_to=())

        solo = make_topology(RoutingStrategy.MESH, make_agent("solo"))
        async with SwarmOrchestrator(solo, ScriptedRunner(flaky), listener=events) as swarm:
            swarm.dispatch_task("1")
            assert await wait_for(lambda: swarm.agents[0].status is S.ERROR)
            assert swarm.is_running
            swarm.dispatch_task("2")
            assert await wait_for(lambda: swarm.agents[0].status is S.IDLE)
        failure = events.of(AgentFailed)[0]
        assert (failure.error, failure.error_type, failure.retryable) == (
            "boom",
            "FlakyError",
            True,
        )
        assert swarm.results[-1].output == "fine"
        assert swarm.metrics.messages_processed == 2

    async def test_error_without_message_uses_type_name(self, events: EventCollector) -> None:
        async def raise_bare(agent: AgentState, message: SwarmMessage) -> AgentReply:
            raise KeyError

        solo = make_topology(RoutingStrategy.MESH, make_agent("solo"))
        async with SwarmOrchestrator(solo, ScriptedRunner(raise_bare), listener=events) as swarm:
            swarm.dispatch_task("x")
            await wait_for(lambda: bool(events.of(AgentFailed)))
        assert events.of(AgentFailed)[0].error == "KeyError"

    async def test_faulty_listener_is_swallowed(
        self, mesh_topology: SwarmTopology, caplog: pytest.LogCaptureFixture
    ) -> None:
        def broken_listener(event: object) -> None:
            raise ValueError("listener bug")

        async with SwarmOrchestrator(
            mesh_topology, ScriptedRunner(final), listener=broken_listener
        ) as swarm:
            swarm.dispatch_task("x")
            await swarm.stop_swarm()
        assert len(swarm.results) == 1
        assert "swarm listener failed" in caplog.text

    async def test_loop_bug_crashes_the_swarm_and_surfaces_on_stop(
        self, mesh_topology: SwarmTopology, caplog: pytest.LogCaptureFixture
    ) -> None:
        async def broken(agent: AgentState, message: SwarmMessage) -> AgentReply:
            return None  # type: ignore[return-value]

        swarm = SwarmOrchestrator(mesh_topology, ScriptedRunner(broken))
        await swarm.start_swarm()
        swarm.dispatch_task("x")
        assert await wait_for(lambda: not swarm.is_running)
        with pytest.raises(SwarmError, match="not running"):
            swarm.dispatch_task("y")
        with pytest.raises(SwarmError, match="crashed") as caught:
            await swarm.stop_swarm()
        assert isinstance(caught.value.__cause__, BaseExceptionGroup)
        assert all(a.status is S.TERMINATED for a in swarm.agents)
        assert any(r.levelno == logging.ERROR and "crashed" in r.message for r in caplog.records)
        assert not leftover_swarm_tasks()


class TestShutdown:
    @staticmethod
    def stuck_runner(cancelled: list[str]) -> ScriptedRunner:
        async def stuck(agent: AgentState, message: SwarmMessage) -> AgentReply:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.append(agent.name)
                raise
            return AgentReply()  # pragma: no cover

        return ScriptedRunner(stuck)

    @pytest.mark.parametrize("how", ["timeout", "no-drain", "abort"])
    async def test_stuck_work_is_cancelled(self, mesh_topology: SwarmTopology, how: str) -> None:
        cancelled: list[str] = []
        swarm = SwarmOrchestrator(mesh_topology, self.stuck_runner(cancelled))
        await swarm.start_swarm()
        swarm.dispatch_task("x")
        swarm.dispatch_task("queued, never started")
        await wait_for(lambda: swarm.metrics.active_agents == 1)
        if how == "timeout":
            drained = await swarm.stop_swarm(drain_timeout=0.05)
        elif how == "no-drain":
            drained = await swarm.stop_swarm(drain=False)
        else:
            stopping = asyncio.create_task(swarm.stop_swarm(drain_timeout=60))
            await asyncio.sleep(0.05)
            assert not stopping.done()
            swarm.abort_drain()
            drained = await asyncio.wait_for(stopping, timeout=5)
        assert drained is False
        assert cancelled == ["m1"]
        assert all(a.status is S.TERMINATED for a in swarm.agents)
        assert not leftover_swarm_tasks()

    async def test_abort_before_stop_skips_the_drain(self, mesh_topology: SwarmTopology) -> None:
        cancelled: list[str] = []
        swarm = SwarmOrchestrator(mesh_topology, self.stuck_runner(cancelled))
        await swarm.start_swarm()
        swarm.dispatch_task("x")
        await wait_for(lambda: swarm.metrics.active_agents == 1)
        swarm.abort_drain()
        assert await asyncio.wait_for(swarm.stop_swarm(drain_timeout=60), timeout=5) is False

    async def test_concurrent_stops_share_one_outcome(self, mesh_topology: SwarmTopology) -> None:
        cancelled: list[str] = []
        swarm = SwarmOrchestrator(mesh_topology, self.stuck_runner(cancelled))
        await swarm.start_swarm()
        swarm.dispatch_task("x")
        await wait_for(lambda: swarm.metrics.active_agents == 1)
        graceful = asyncio.create_task(swarm.stop_swarm(drain_timeout=60))
        await asyncio.sleep(0.02)
        hard = await asyncio.wait_for(swarm.stop_swarm(drain=False), timeout=5)  # aborts the drain
        assert (await graceful, hard) == (False, False)
        assert not leftover_swarm_tasks()
        assert all(a.status is S.TERMINATED for a in swarm.agents)

    async def test_context_manager_cancels_on_error(self, mesh_topology: SwarmTopology) -> None:
        cancelled: list[str] = []
        swarm = SwarmOrchestrator(mesh_topology, self.stuck_runner(cancelled))

        async def failing_body() -> None:
            async with swarm:
                swarm.dispatch_task("x")
                await wait_for(lambda: swarm.metrics.active_agents == 1)
                raise RuntimeError("body failed")

        with pytest.raises(RuntimeError, match="body failed"):
            await failing_body()
        assert cancelled == ["m1"]
        assert not leftover_swarm_tasks()


class TestStatusHook:
    async def test_runner_toggles_waiting_and_running(
        self, mesh_topology: SwarmTopology, events: EventCollector
    ) -> None:
        class Aware:
            hook: StatusHook | None = None

            def bind_status_hook(self, hook: StatusHook) -> None:
                self.hook = hook

            async def run(self, agent: AgentState, message: SwarmMessage) -> AgentReply:
                assert self.hook is not None
                self.hook(agent.id, S.WAITING)
                self.hook(agent.id, S.WAITING)  # idempotent
                if message.payload == "resume":
                    self.hook(agent.id, S.RUNNING)
                return AgentReply(output="ok", forward_to=())  # may return while WAITING

        runner = Aware()
        target = mesh_topology.agents[0].id
        async with SwarmOrchestrator(mesh_topology, runner, listener=events) as swarm:
            assert runner.hook is not None  # bound automatically
            for payload in ("resume", "leave-waiting"):
                swarm.dispatch_task(payload, agent_id=target)
            assert await swarm.stop_swarm()
        statuses = [e.agent.status for e in events.of(AgentUpdated) if e.agent.id == target]
        assert statuses.count(S.WAITING) == 2
        assert S.IDLE in statuses  # WAITING was normalised before completing
        assert len(swarm.results) == 2

    async def test_hook_rejects_lifecycle_changes(self, mesh_topology: SwarmTopology) -> None:
        swarm = SwarmOrchestrator(mesh_topology, ScriptedRunner(final))
        idle = mesh_topology.agents[0].id
        with pytest.raises(InvalidStateTransitionError, match="toggle between running and waiting"):
            swarm.signal_status(idle, S.WAITING)
        with pytest.raises(InvalidStateTransitionError):
            swarm.signal_status(idle, S.IDLE)
        with pytest.raises(AgentNotFoundError):
            swarm.signal_status(uuid4(), S.WAITING)


class TestConstruction:
    def test_rejects_running_or_waiting_initial_agents(self) -> None:
        running = transition(make_agent("r"), S.RUNNING, task="t")
        with pytest.raises(SwarmError, match="must not start running"):
            SwarmOrchestrator(make_topology(RoutingStrategy.MESH, running), ScriptedRunner(final))

    @pytest.mark.parametrize("cost", [math.nan, math.inf, -1.0])
    def test_rejects_bad_cost(self, mesh_topology: SwarmTopology, cost: float) -> None:
        with pytest.raises(ValueError, match="cost_per_million_tokens"):
            SwarmOrchestrator(mesh_topology, ScriptedRunner(final), cost_per_million_tokens=cost)

    async def test_latency_uses_the_injected_clock(self) -> None:
        ticks = iter([10.0, 10.25])
        solo = make_topology(RoutingStrategy.MESH, make_agent("solo"))
        async with SwarmOrchestrator(
            solo, ScriptedRunner(final), clock=lambda: next(ticks)
        ) as swarm:
            swarm.dispatch_task("x")
            await swarm.stop_swarm()
        assert swarm.agents[0].latency_ms == pytest.approx(250.0)


class TestAgentReply:
    @pytest.mark.parametrize(
        ("kwargs", "error"),
        [
            ({"forward_to": (uuid4(),)}, "require an output"),
            ({"broadcast": True}, "require an output"),
            ({"output": "x", "broadcast": True, "forward_to": ()}, "mutually exclusive"),
            ({"output": "x", "forward_to": (UUID(int=1), UUID(int=1))}, "duplicates"),
            ({"tokens_used": -1}, "greater than or equal"),
        ],
    )
    def test_validation(self, kwargs: dict[str, object], error: str) -> None:
        with pytest.raises(ValidationError, match=error):
            AgentReply(**kwargs)  # type: ignore[arg-type]
