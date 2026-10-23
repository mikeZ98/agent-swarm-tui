"""Mailboxes and topology-enforcing routing for every strategy."""

import asyncio
from uuid import UUID, uuid4

import pytest

from agent_swarm_tui.core import AgentNotFoundError, MessageRouter, RoutingError
from agent_swarm_tui.schemas import MessageType, RoutingStrategy, SwarmMessage, SwarmTopology
from tests.conftest import make_agent, make_topology

D, B, SYS = MessageType.DIRECT, MessageType.BROADCAST, MessageType.SYSTEM


def msg(sender: UUID, kind: MessageType, target: UUID | None = None) -> SwarmMessage:
    return SwarmMessage(sender_id=sender, target_id=target, payload="x", message_type=kind)


def ids(topology: SwarmTopology) -> list[UUID]:
    return [agent.id for agent in topology.agents]


class TestSequential:
    def test_direct_only_to_the_next_step(self, sequential_topology: SwarmTopology) -> None:
        router = MessageRouter(sequential_topology)
        p1, p2, p3 = ids(sequential_topology)
        assert router.recipients(msg(p1, D, p2)) == (p2,)
        assert router.recipients(msg(p2, D, p3)) == (p3,)
        for sender, target in ((p1, p3), (p2, p1), (p3, p1)):
            with pytest.raises(RoutingError, match="next pipeline step"):
                router.recipients(msg(sender, D, target))

    def test_broadcast_rejected(self, sequential_topology: SwarmTopology) -> None:
        with pytest.raises(RoutingError, match="does not allow broadcasts"):
            MessageRouter(sequential_topology).recipients(msg(ids(sequential_topology)[0], B))

    def test_successor_defaults_and_entry_point(self, sequential_topology: SwarmTopology) -> None:
        router = MessageRouter(sequential_topology)
        p1, p2, p3 = ids(sequential_topology)
        assert (router.successor(p1), router.successor(p3)) == (p2, None)
        assert router.default_targets(p1) == (p2,)
        assert router.default_targets(p3) == ()
        assert router.entry_point == p1


class TestHierarchical:
    def test_hub_and_spoke(self, hierarchical_topology: SwarmTopology) -> None:
        router = MessageRouter(hierarchical_topology)
        w1, boss, w2 = ids(hierarchical_topology)
        assert router.entry_point == boss
        assert router.recipients(msg(boss, D, w1)) == (w1,)
        assert router.recipients(msg(w2, D, boss)) == (boss,)
        with pytest.raises(RoutingError, match="workers may only message the orchestrator"):
            router.recipients(msg(w1, D, w2))

    def test_only_the_orchestrator_broadcasts(self, hierarchical_topology: SwarmTopology) -> None:
        router = MessageRouter(hierarchical_topology)
        w1, boss, w2 = ids(hierarchical_topology)
        assert router.recipients(msg(boss, B)) == (w1, w2)
        with pytest.raises(RoutingError, match="only the orchestrator may broadcast"):
            router.recipients(msg(w1, B))

    def test_defaults(self, hierarchical_topology: SwarmTopology) -> None:
        router = MessageRouter(hierarchical_topology)
        w1, boss, _ = ids(hierarchical_topology)
        assert router.default_targets(w1) == (boss,)
        assert router.default_targets(boss) == ()


class TestMesh:
    def test_any_peer(self, mesh_topology: SwarmTopology) -> None:
        router = MessageRouter(mesh_topology)
        m1, m2, m3 = ids(mesh_topology)
        assert router.recipients(msg(m3, D, m1)) == (m1,)
        assert router.recipients(msg(m2, B)) == (m1, m3)
        assert router.default_targets(m1) == ()
        assert router.entry_point == m1

    def test_single_agent_broadcast_reaches_nobody(self) -> None:
        solo = make_topology(RoutingStrategy.MESH, make_agent("solo"))
        assert MessageRouter(solo).route(msg(solo.agents[0].id, B)) == ()


class TestSystemMessages:
    def test_from_runtime_untargeted_and_targeted(self, sequential_topology: SwarmTopology) -> None:
        router = MessageRouter(sequential_topology)
        swarm_id = sequential_topology.swarm_id
        assert router.recipients(msg(swarm_id, SYS)) == tuple(ids(sequential_topology))
        last = ids(sequential_topology)[2]
        assert router.recipients(msg(swarm_id, SYS, last)) == (last,)  # bypasses the pipeline

    def test_must_come_from_the_runtime(self, mesh_topology: SwarmTopology) -> None:
        with pytest.raises(RoutingError, match="swarm runtime"):
            MessageRouter(mesh_topology).recipients(msg(ids(mesh_topology)[0], SYS))

    def test_unknown_target(self, mesh_topology: SwarmTopology) -> None:
        with pytest.raises(AgentNotFoundError):
            MessageRouter(mesh_topology).recipients(msg(mesh_topology.swarm_id, SYS, uuid4()))


class TestUnknownAgents:
    def test_errors_are_typed_lookups(self, mesh_topology: SwarmTopology) -> None:
        router = MessageRouter(mesh_topology)
        stranger = uuid4()
        for call in (
            lambda: router.recipients(msg(stranger, B)),
            lambda: router.recipients(msg(ids(mesh_topology)[0], D, stranger)),
            lambda: router.mailbox(stranger),
            lambda: router.successor(stranger),
            lambda: router.default_targets(stranger),
        ):
            with pytest.raises(AgentNotFoundError) as caught:
                call()
            assert caught.value.agent_id == stranger
            assert isinstance(caught.value, LookupError)


class TestDelivery:
    async def test_route_enqueues_for_recipients(self, mesh_topology: SwarmTopology) -> None:
        router = MessageRouter(mesh_topology)
        m1, m2, m3 = ids(mesh_topology)
        message = msg(m1, B)
        assert router.route(message) == (m2, m3)
        assert router.mailbox(m2).get_nowait() is message
        assert router.mailbox(m3).get_nowait() is message
        assert router.mailbox(m1).empty()

    def test_batch_over_capacity_is_atomic(self, mesh_topology: SwarmTopology) -> None:
        router = MessageRouter(mesh_topology, mailbox_size=2)
        m1, m2, m3 = ids(mesh_topology)
        router.route(msg(m1, D, m2))
        with pytest.raises(RoutingError, match="is full"):
            router.route_batch([msg(m1, D, m3), msg(m1, D, m2), msg(m3, D, m2)])
        assert router.mailbox(m2).qsize() == 1
        assert router.mailbox(m3).qsize() == 0

    def test_invalid_message_in_batch_enqueues_nothing(
        self, sequential_topology: SwarmTopology
    ) -> None:
        router = MessageRouter(sequential_topology)
        p1, p2, p3 = ids(sequential_topology)
        with pytest.raises(RoutingError):
            router.route_batch([msg(p1, D, p2), msg(p1, D, p3)])
        assert router.mailbox(p2).empty()

    def test_mailbox_size_must_be_positive(self, mesh_topology: SwarmTopology) -> None:
        with pytest.raises(ValueError, match="mailbox_size"):
            MessageRouter(mesh_topology, mailbox_size=0)

    async def test_shutdown_drains_then_closes(self, mesh_topology: SwarmTopology) -> None:
        router = MessageRouter(mesh_topology)
        m1, m2, _ = ids(mesh_topology)
        router.route(msg(m1, D, m2))
        router.shutdown()
        router.shutdown()  # idempotent
        assert router.is_closed
        assert router.topology is mesh_topology
        with pytest.raises(RoutingError, match="shut down"):
            router.route(msg(m1, D, m2))
        mailbox = router.mailbox(m2)
        assert (await mailbox.get()).sender_id == m1  # queued message still delivered
        with pytest.raises(asyncio.QueueShutDown):
            await mailbox.get()

    async def test_immediate_shutdown_discards(self, mesh_topology: SwarmTopology) -> None:
        router = MessageRouter(mesh_topology)
        m1, m2, _ = ids(mesh_topology)
        router.route(msg(m1, D, m2))
        router.shutdown(immediate=True)
        with pytest.raises(asyncio.QueueShutDown):
            await router.mailbox(m2).get()
