"""Domain model invariants: strictness, immutability, bounds and cross-field contracts."""

import math
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_swarm_tui import schemas
from agent_swarm_tui.schemas import (
    MAX_AGENTS,
    MAX_PAYLOAD_CHARS,
    MAX_TASK_CHARS,
    AgentRole,
    AgentState,
    AgentStatus,
    MessageType,
    RoutingStrategy,
    SwarmMessage,
    SwarmMetrics,
    SwarmTopology,
)
from tests.conftest import make_agent, make_topology

R = AgentRole.EXECUTOR


class TestAgentState:
    def test_defaults(self) -> None:
        agent = AgentState(name="a", role=R)
        assert (agent.status, agent.current_task, agent.total_tokens, agent.latency_ms) == (
            AgentStatus.IDLE,
            None,
            0,
            0.0,
        )
        assert agent.id != AgentState(name="a", role=R).id

    def test_frozen_and_hashable(self) -> None:
        agent = AgentState(name="a", role=R)
        with pytest.raises(ValidationError):
            agent.status = AgentStatus.RUNNING  # type: ignore[misc]
        assert hash(agent) == hash(agent.model_copy())

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"foo": 1},  # extra field
            {"total_tokens": "5"},  # no str -> int coercion
            {"total_tokens": True},  # bool is not an int in strict mode
            {"total_tokens": -1},
            {"latency_ms": math.nan},
            {"latency_ms": math.inf},
            {"latency_ms": -0.5},
            {"role": "executor"},  # raw str, not the enum (python mode)
            {"id": str(uuid4())},  # raw str, not a UUID (python mode)
        ],
    )
    def test_rejects_invalid_fields(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(ValidationError):
            AgentState(**{"name": "a", "role": R, **kwargs})

    @pytest.mark.parametrize("name", ["", "-leading-dash", "new\nline", "[bold]x", "x" * 65])
    def test_rejects_bad_names(self, name: str) -> None:
        with pytest.raises(ValidationError):
            AgentState(name=name, role=R)

    def test_int_accepted_for_float(self) -> None:
        assert AgentState(name="a", role=R, latency_ms=5).latency_ms == 5.0

    def test_running_requires_task(self) -> None:
        with pytest.raises(ValidationError, match="requires a current_task"):
            AgentState(name="a", role=R, status=AgentStatus.RUNNING)
        assert AgentState(name="a", role=R, status=AgentStatus.RUNNING, current_task="t")

    @pytest.mark.parametrize("status", [AgentStatus.IDLE, AgentStatus.TERMINATED])
    def test_idle_and_terminated_forbid_task(self, status: AgentStatus) -> None:
        with pytest.raises(ValidationError, match="must not carry a current_task"):
            AgentState(name="a", role=R, status=status, current_task="t")

    @pytest.mark.parametrize("status", [AgentStatus.WAITING, AgentStatus.ERROR])
    def test_waiting_and_error_allow_either(self, status: AgentStatus) -> None:
        assert AgentState(name="a", role=R, status=status).current_task is None
        assert AgentState(name="a", role=R, status=status, current_task="t").current_task == "t"

    def test_task_length_bound(self) -> None:
        AgentState(name="a", role=R, status=AgentStatus.ERROR, current_task="x" * MAX_TASK_CHARS)
        with pytest.raises(ValidationError):
            AgentState(
                name="a", role=R, status=AgentStatus.ERROR, current_task="x" * (MAX_TASK_CHARS + 1)
            )

    def test_json_round_trip_in_strict_mode(self) -> None:
        agent = AgentState(name="a", role=R, status=AgentStatus.RUNNING, current_task="t")
        assert AgentState.model_validate_json(agent.model_dump_json()) == agent


class TestSwarmMessage:
    def test_direct_requires_distinct_target(self) -> None:
        sender = uuid4()
        with pytest.raises(ValidationError, match="require a target_id"):
            SwarmMessage(sender_id=sender, payload="p", message_type=MessageType.DIRECT)
        with pytest.raises(ValidationError, match="cannot target their sender"):
            SwarmMessage(
                sender_id=sender, target_id=sender, payload="p", message_type=MessageType.DIRECT
            )

    def test_broadcast_forbids_target(self) -> None:
        with pytest.raises(ValidationError, match="must not set a target_id"):
            SwarmMessage(
                sender_id=uuid4(),
                target_id=uuid4(),
                payload="p",
                message_type=MessageType.BROADCAST,
            )

    def test_system_target_optional(self) -> None:
        assert SwarmMessage(sender_id=uuid4(), payload="p", message_type=MessageType.SYSTEM)
        assert SwarmMessage(
            sender_id=uuid4(), target_id=uuid4(), payload="p", message_type=MessageType.SYSTEM
        )

    @pytest.mark.parametrize("payload", ["", "x" * (MAX_PAYLOAD_CHARS + 1)])
    def test_payload_bounds(self, payload: str) -> None:
        with pytest.raises(ValidationError):
            SwarmMessage(sender_id=uuid4(), payload=payload, message_type=MessageType.SYSTEM)

    def test_timestamp_must_be_aware_and_defaults_to_utc(self) -> None:
        with pytest.raises(ValidationError):
            SwarmMessage(
                sender_id=uuid4(),
                payload="p",
                message_type=MessageType.SYSTEM,
                timestamp=datetime(2026, 1, 1),
            )
        message = SwarmMessage(sender_id=uuid4(), payload="p", message_type=MessageType.SYSTEM)
        assert message.timestamp.tzinfo is UTC

    def test_json_round_trip(self) -> None:
        message = SwarmMessage(
            sender_id=uuid4(), target_id=uuid4(), payload="p", message_type=MessageType.DIRECT
        )
        assert SwarmMessage.model_validate_json(message.model_dump_json()) == message


class TestSwarmTopology:
    def test_lookup(self, hierarchical_topology: SwarmTopology) -> None:
        worker = hierarchical_topology.agents[0]
        assert hierarchical_topology.agent(worker.id) == worker
        with pytest.raises(KeyError):
            hierarchical_topology.agent(uuid4())

    def test_roster_bounds(self) -> None:
        with pytest.raises(ValidationError):
            make_topology(RoutingStrategy.MESH)
        with pytest.raises(ValidationError):
            make_topology(
                RoutingStrategy.MESH, *(make_agent(f"a{i}") for i in range(MAX_AGENTS + 1))
            )

    def test_roster_must_be_a_tuple_in_python_mode(self) -> None:
        with pytest.raises(ValidationError):
            SwarmTopology(
                name="t",
                agents=[make_agent("a")],  # type: ignore[arg-type]
                routing_strategy=RoutingStrategy.MESH,
            )

    def test_unique_ids_and_case_insensitive_names(self) -> None:
        agent = make_agent("alpha")
        with pytest.raises(ValidationError, match="ids must be unique"):
            make_topology(RoutingStrategy.MESH, agent, agent)
        with pytest.raises(ValidationError, match="names must be unique"):
            make_topology(RoutingStrategy.MESH, agent, make_agent("ALPHA"))

    @pytest.mark.parametrize("orchestrators", [0, 2])
    def test_hierarchical_needs_exactly_one_orchestrator(self, orchestrators: int) -> None:
        agents = [make_agent(f"o{i}", AgentRole.ORCHESTRATOR) for i in range(orchestrators)]
        with pytest.raises(ValidationError, match="exactly one orchestrator"):
            make_topology(RoutingStrategy.HIERARCHICAL, *agents, make_agent("w"))

    def test_json_round_trip_and_hash(self, mesh_topology: SwarmTopology) -> None:
        assert SwarmTopology.model_validate_json(mesh_topology.model_dump_json()) == mesh_topology
        assert hash(mesh_topology)


class TestSwarmMetrics:
    def test_defaults(self) -> None:
        assert SwarmMetrics() == SwarmMetrics(
            active_agents=0, messages_processed=0, total_tokens=0, estimated_cost_usd=0.0
        )

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"active_agents": -1},
            {"messages_processed": -1},
            {"total_tokens": -1},
            {"estimated_cost_usd": -0.01},
            {"estimated_cost_usd": math.nan},
        ],
    )
    def test_bounds(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(ValidationError):
            SwarmMetrics(**kwargs)


def test_public_exports_resolve() -> None:
    assert schemas.__all__
    assert all(hasattr(schemas, name) for name in schemas.__all__)
