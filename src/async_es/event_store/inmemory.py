from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from async_es.protocols import EventStore

if TYPE_CHECKING:
    from async_es.event import DomainEvent
    from async_es.types import AggregateId


class InMemoryEventStore(EventStore):
    def __init__(self) -> None:
        # key: (aggregate_type, aggregate_id.hex) -> list of events
        self._streams: dict[tuple[str, str], list[DomainEvent[Any, Any]]] = defaultdict(list)

    async def append(self, events: list[DomainEvent[Any, Any]]) -> None:
        for evt in events:
            key = (evt.aggregate_type, evt.aggregate_id.hex)
            self._streams[key].append(evt)

    async def load(self, aggregate_type: str, aggregate_id: AggregateId) -> list[DomainEvent[Any, Any]]:
        key = (aggregate_type, aggregate_id.hex)
        return list(self._streams.get(key, ()))
