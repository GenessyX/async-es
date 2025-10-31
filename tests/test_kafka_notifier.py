from __future__ import annotations

import asyncio
import dataclasses
import json
import os
from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
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


async def _wait_kafka_ready(bootstrap: str) -> None:
    started = False
    for _ in range(30):
        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        try:
            await producer.start()
            await producer.stop()
            started = True
            break
        except Exception:  # noqa: BLE001
            await asyncio.sleep(1)
    if not started:
        msg = "Kafka not ready"
        raise RuntimeError(msg)


@pytest_asyncio.fixture
async def kafka_bootstrap() -> AsyncIterator[str]:
    env_bootstrap = os.getenv("KAFKA_BOOTSTRAP")
    test_topic = os.getenv("KAFKA_TEST_TOPIC", "test-events")

    if env_bootstrap:
        await _wait_kafka_ready(env_bootstrap)
        # Pre-clean topic
        admin = AIOKafkaAdminClient(bootstrap_servers=env_bootstrap)
        await admin.start()
        try:
            with suppress(Exception):
                await admin.delete_topics([test_topic])
            with suppress(Exception):
                await admin.create_topics([NewTopic(name=test_topic, num_partitions=1, replication_factor=1)])
        finally:
            await admin.close()

        try:
            yield env_bootstrap
        finally:
            # Post-clean topic
            admin2 = AIOKafkaAdminClient(bootstrap_servers=env_bootstrap)
            await admin2.start()
            try:
                with suppress(Exception):
                    await admin2.delete_topics([test_topic])
                with suppress(Exception):
                    await admin2.create_topics([NewTopic(name=test_topic, num_partitions=1, replication_factor=1)])
            finally:
                await admin2.close()
    else:
        with KafkaContainer("confluentinc/cp-kafka:7.6.0") as kafka:
            bootstrap_servers = kafka.get_bootstrap_server()
            await asyncio.sleep(1)
            await _wait_kafka_ready(bootstrap_servers)
            yield bootstrap_servers


@pytest.mark.asyncio
async def test_kafka_notifier_sends_events(kafka_bootstrap: str) -> None:
    topic = os.getenv("KAFKA_TEST_TOPIC", "test-events")
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
