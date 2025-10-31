from __future__ import annotations

import asyncio
import dataclasses
import json
from typing import TYPE_CHECKING, Any, cast

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer
from testcontainers.kafka import KafkaContainer  # type: ignore[import-untyped]

from async_es.event import DomainEvent
from async_es.notifiers.kafka import KafkaNotifier
from async_es.types import AggregateId

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class FooId(AggregateId): ...


@dataclasses.dataclass
class Payload:
    x: int


@pytest_asyncio.fixture
async def kafka_bootstrap() -> AsyncIterator[str]:
    # Start a Kafka container and yield its bootstrap servers URL
    with KafkaContainer("confluentinc/cp-kafka:7.6.0") as kafka:
        bootstrap_servers = kafka.get_bootstrap_server()
        # Give broker a moment to settle
        await asyncio.sleep(1)
        yield bootstrap_servers


@pytest.mark.asyncio
async def test_kafka_notifier_sends_events(kafka_bootstrap: str) -> None:
    topic = "test-events"
    notifier = KafkaNotifier(bootstrap_servers=kafka_bootstrap, topic=topic)

    eid = FooId.generate()
    events: list[DomainEvent[FooId, Payload]] = [
        DomainEvent(
            aggregate_type="Foo",
            aggregate_id=eid,
            event_type="payload_added",
            payload=Payload(x=1),
        ),
        DomainEvent(
            aggregate_type="Foo",
            aggregate_id=eid,
            event_type="payload_added",
            payload=Payload(x=2),
        ),
    ]

    await notifier.notify(events)
    await notifier.close()

    # Consume back to verify

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap,
        group_id="test-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        received: list[dict[str, Any]] = []
        # Collect up to 2 messages or timeout
        async for msg in consumer:
            assert msg.value
            received.append(cast("dict[str, Any]", json.loads(msg.value)))
            if len(received) >= 2:
                break
    finally:
        await consumer.stop()

    assert len(received) == 2
    assert {r["event_type"] for r in received} == {"payload_added"}
    assert {r["payload"]["x"] for r in received} == {1, 2}
    assert all(r["aggregate_type"] == "Foo" for r in received)
    assert all(r["aggregate_id"] == str(eid) for r in received)
