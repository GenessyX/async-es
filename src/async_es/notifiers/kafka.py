from __future__ import annotations

from typing import TYPE_CHECKING, Any

from async_es.protocols import ApplicationNotifier

if TYPE_CHECKING:
    from async_es.event import DomainEvent


class KafkaNotifier(ApplicationNotifier):
    def __init__(self, *, topic: str) -> None:
        self._topic = topic
        # Placeholder for a real Kafka producer

    async def notify(self, events: list[DomainEvent[Any, Any]]) -> None:
        # Real implementation would serialize and produce to Kafka
        _ = (self._topic, events)
