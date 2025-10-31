from __future__ import annotations

import dataclasses
import json
from typing import TYPE_CHECKING, Any

from aiokafka import AIOKafkaProducer

from async_es.protocols import ApplicationNotifier

if TYPE_CHECKING:
    from collections.abc import Callable

    from async_es.event import DomainEvent


def _event_to_json(event: "DomainEvent[Any, Any]") -> bytes:
    payload = event.payload
    payload_obj: Any
    if dataclasses.is_dataclass(payload) and not isinstance(payload, type):
        payload_obj = dataclasses.asdict(payload)
    else:
        payload_obj = payload
    data = {
        "id": str(event.id),
        "aggregate_type": event.aggregate_type,
        "aggregate_id": str(event.aggregate_id),
        "event_type": event.event_type,
        "payload": payload_obj,
        "occurred_at": event.occurred_at.isoformat(),
        "published_at": event.published_at.isoformat() if event.published_at else None,
        "metadata": event.metadata,
    }
    return json.dumps(data, separators=(",", ":")).encode()


class KafkaNotifier(ApplicationNotifier):
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        key: "Callable[[DomainEvent[Any, Any]], bytes] | None" = None,
        client_id: str | None = None,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._key_fn = key
        self._client_id = client_id
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        if self._producer is not None:
            return
        producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            value_serializer=lambda v: v,
            key_serializer=lambda v: v,
        )
        await producer.start()
        self._producer = producer

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()  # pyright: ignore[reportAttributeAccessIssue]
            self._producer = None

    async def notify(self, events: list["DomainEvent[Any, Any]"]) -> None:
        if not events:
            return
        if self._producer is None:
            await self.start()
        if self._producer is None:
            msg = "Kafka producer failed to start"
            raise RuntimeError(msg)
        for e in events:
            key = self._key_fn(e) if self._key_fn else None
            await self._producer.send_and_wait(self._topic, _event_to_json(e), key=key)  # pyright: ignore[reportAttributeAccessIssue]
