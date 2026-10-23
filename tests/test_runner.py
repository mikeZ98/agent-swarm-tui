"""GatewayAgentRunner: prompting, the WAITING hook, RunnerOutput and reply policies."""

import json
from uuid import UUID, uuid4

import pytest

from agent_swarm_tui.client import (
    DEFAULT_PERSONAS,
    GatewayAgentRunner,
    GatewayClient,
    GatewayHTTPError,
    InferenceOptions,
    PromptTemplate,
    RunnerOutput,
    delegating_reply,
    next_hop_reply,
)
from agent_swarm_tui.client.runner import MAX_TEMPLATE_CHARS, TRUNCATION_MARKER
from agent_swarm_tui.core import (
    AgentUpdated,
    InvalidStateTransitionError,
    StatusAwareRunner,
    SwarmOrchestrator,
)
from agent_swarm_tui.schemas import (
    MAX_PAYLOAD_CHARS,
    AgentRole,
    AgentState,
    AgentStatus,
    MessageType,
    RoutingStrategy,
    SwarmMessage,
)
from tests.conftest import (
    EventCollector,
    MockGateway,
    Reply,
    good_stream,
    make_agent,
    make_topology,
    rate_limit_headers,
    wait_for,
)

S = AgentStatus


def running(name: str = "r1", role: AgentRole = AgentRole.RESEARCHER) -> AgentState:
    return AgentState(name=name, role=role, status=S.RUNNING, current_task="the task")


def system_message(payload: str = "find it") -> SwarmMessage:
    return SwarmMessage(sender_id=uuid4(), payload=payload, message_type=MessageType.SYSTEM)


def output(content: str = "result", **overrides: object) -> RunnerOutput:
    fields: dict[str, object] = {
        "agent_id": uuid4(),
        "content": content,
        "tokens_used": 3,
        "latency_ms": 1.0,
        "model_used": "m",
        "provider": "p",
        "cached_hit": False,
    }
    return RunnerOutput.model_validate({**fields, **overrides})


class TestPromptTemplate:
    @pytest.mark.parametrize(
        ("source", "error"),
        [
            ("hello $name", "must include \\$payload"),
            ("$payload $secret", "unknown template fields: secret"),
            ("$payload costs $5", "invalid '\\$' placeholder"),
            ("$payload" + "x" * MAX_TEMPLATE_CHARS, "exceeds"),
        ],
    )
    def test_rejected(self, source: str, error: str) -> None:
        with pytest.raises(ValueError, match=error):
            PromptTemplate(source)

    def test_render_is_single_pass(self) -> None:
        template = PromptTemplate("[$role/$name/$message_type] $persona :: $task :: $payload $$")
        rendered = template.render(running(), system_message("costs $HOME ${name}"), persona="P")
        assert rendered == "[researcher/r1/system] P :: the task :: costs $HOME ${name} $"

    def test_default_template_uses_role_personas(self, client: GatewayClient) -> None:
        runner = GatewayAgentRunner(client, personas={AgentRole.EXECUTOR: "Custom persona."})
        prompt = runner.render_prompt(running("x", AgentRole.EXECUTOR), system_message("do it"))
        assert prompt.startswith("You are x, the executor agent in a multi-agent swarm.")
        assert "Custom persona." in prompt
        assert prompt.endswith("do it")
        reviewer = runner.render_prompt(running("y", AgentRole.REVIEWER), system_message())
        assert DEFAULT_PERSONAS[AgentRole.REVIEWER] in reviewer


class TestComplete:
    async def test_output_from_summary_and_callbacks(
        self, gateway: MockGateway, client: GatewayClient
    ) -> None:
        gateway.responder = lambda body: Reply(
            chunks=good_stream("Hel", "lo", tokens_used=42, latency_ms=12.5, cached_hit=True),
            headers=rate_limit_headers(9),
        )
        deltas: list[tuple[UUID, str]] = []
        outputs: list[RunnerOutput] = []
        runner = GatewayAgentRunner(
            client,
            options=InferenceOptions(max_tokens=64, model="m-1"),
            on_delta=lambda agent_id, text: deltas.append((agent_id, text)),
            on_output=outputs.append,
        )
        agent = running()
        result = await runner.complete(agent, system_message())
        assert (result.content, result.tokens_used, result.latency_ms, result.cached_hit) == (
            "Hello",
            42,
            12.5,
            True,
        )
        assert (result.model_used, result.provider, result.agent_id) == (
            "stub-model",
            "vllm",
            agent.id,
        )
        assert result.rate_limit is not None
        assert result.rate_limit.remaining == 9
        assert deltas == [(agent.id, "Hel"), (agent.id, "lo")]
        assert outputs == [result]
        body = gateway.bodies[0]
        assert (body["max_tokens"], body["model"], body["stream"]) == (64, "m-1", True)
        assert "temperature" not in body

    async def test_callback_failures_are_swallowed(
        self, client: GatewayClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        def explode(*_args: object) -> None:
            raise RuntimeError("ui bug")

        runner = GatewayAgentRunner(client, on_delta=explode, on_output=explode)
        assert (await runner.complete(running(), system_message())).content == "r1 says done"
        assert "on_delta callback failed" in caplog.text
        assert "on_output callback failed" in caplog.text

    async def test_prompt_contract_violation(
        self, gateway: MockGateway, client: GatewayClient
    ) -> None:
        # Payload (<= 65,536) + template (<= 8,192) can never exceed the 100,000-char prompt
        # limit, so the reachable violation is a blank prompt: whitespace into "$payload".
        runner = GatewayAgentRunner(client, template=PromptTemplate("$payload"))
        with pytest.raises(ValueError, match="prompt must not be blank"):
            await runner.complete(running(), system_message("   "))
        assert gateway.requests == []


class TestStatusHook:
    async def test_waiting_until_first_token_then_running(self, client: GatewayClient) -> None:
        signals: list[AgentStatus] = []
        runner = GatewayAgentRunner(client)
        assert isinstance(runner, StatusAwareRunner)
        runner.bind_status_hook(lambda agent_id, status: signals.append(status))
        await runner.complete(running(), system_message())
        assert signals == [S.WAITING, S.RUNNING]

    async def test_restores_running_when_the_call_fails(
        self, gateway: MockGateway, client: GatewayClient
    ) -> None:
        gateway.responder = lambda body: Reply(
            status=503, json_body={"error": {"code": "no_route", "message": "x"}}
        )
        signals: list[AgentStatus] = []
        runner = GatewayAgentRunner(client)
        runner.bind_status_hook(lambda agent_id, status: signals.append(status))
        with pytest.raises(GatewayHTTPError):
            await runner.complete(running(), system_message())
        assert signals == [S.WAITING, S.RUNNING]

    async def test_rejected_hook_is_tolerated(self, client: GatewayClient) -> None:
        def reject(agent_id: UUID, status: AgentStatus) -> None:
            raise InvalidStateTransitionError(agent_id, S.IDLE, status)

        runner = GatewayAgentRunner(client)
        runner.bind_status_hook(reject)
        assert (await runner.complete(running(), system_message())).content

    async def test_inside_a_swarm(
        self, gateway: MockGateway, client: GatewayClient, events: EventCollector
    ) -> None:
        gateway.responder = lambda body: Reply(chunks=good_stream("a", "b"), ttft=0.05)
        solo = make_topology(RoutingStrategy.MESH, make_agent("solo"))
        async with SwarmOrchestrator(solo, GatewayAgentRunner(client), listener=events) as swarm:
            swarm.dispatch_task("go")
            assert await wait_for(lambda: swarm.agents[0].status is S.WAITING)
            assert swarm.metrics.active_agents == 1
            assert await swarm.stop_swarm()
        statuses = [e.agent.status for e in events.of(AgentUpdated)]
        assert statuses[:3] == [S.RUNNING, S.WAITING, S.RUNNING]
        assert statuses[-2:] == [S.IDLE, S.TERMINATED]
        assert swarm.agents[0].total_tokens == 10
        assert swarm.results[-1].output == "ab"
        assert json.loads(gateway.requests[0].content)["prompt"].startswith("You are solo,")

    async def test_cancel_while_waiting(self, gateway: MockGateway, client: GatewayClient) -> None:
        gateway.responder = lambda body: Reply(chunks=good_stream("a"), ttft=30)
        solo = make_topology(RoutingStrategy.MESH, make_agent("solo"))
        swarm = SwarmOrchestrator(solo, GatewayAgentRunner(client))
        await swarm.start_swarm()
        swarm.dispatch_task("x")
        assert await wait_for(lambda: swarm.agents[0].status is S.WAITING)
        assert await swarm.stop_swarm(drain=False) is False
        assert swarm.agents[0].status is S.TERMINATED
        assert gateway.cancelled == 1


class TestReplyPolicies:
    def test_next_hop(self) -> None:
        reply = next_hop_reply(running(), system_message(), output("text"))
        assert (reply.output, reply.tokens_used, reply.broadcast, reply.forward_to) == (
            "text",
            3,
            False,
            None,
        )

    def test_delegating_broadcasts_only_new_tasks_from_the_orchestrator(self) -> None:
        boss = running("boss", AgentRole.ORCHESTRATOR)
        report = SwarmMessage(
            sender_id=uuid4(), target_id=boss.id, payload="r", message_type=MessageType.DIRECT
        )
        assert delegating_reply(boss, system_message(), output("plan")).broadcast
        assert not delegating_reply(boss, report, output("synthesis")).broadcast
        assert not delegating_reply(running(), system_message(), output("x")).broadcast

    @pytest.mark.parametrize("content", ["", "   \n"])
    def test_blank_output_is_not_forwarded(self, content: str) -> None:
        boss = running("boss", AgentRole.ORCHESTRATOR)
        assert delegating_reply(boss, system_message(), output(content)).output is None
        assert next_hop_reply(boss, system_message(), output(content)).output is None

    def test_oversized_output_is_truncated_with_marker(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        clipped = next_hop_reply(running(), system_message(), output("x" * 70_000)).output
        assert clipped is not None
        assert len(clipped) == MAX_PAYLOAD_CHARS
        assert clipped.endswith(TRUNCATION_MARKER)
        assert "truncated" in caplog.text
