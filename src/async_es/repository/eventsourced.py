from __future__ import annotations

from typing import TYPE_CHECKING, Any

from async_es.protocols import EventStore, Repository
from async_es.types import AggregateId

if TYPE_CHECKING:
    from collections.abc import Callable

    from async_es.aggregate import Aggregate
    from async_es.event import DomainEvent


class EventSourcedRepository[IdT: AggregateId](Repository[IdT]):
    def __init__(
        self,
        *,
        aggregate_type: str,
        event_store: EventStore,
        factory: Callable[[IdT], Aggregate[IdT]],
        apply: Callable[[Aggregate[IdT], DomainEvent[IdT, Any]], None],
    ) -> None:
        self._aggregate_type = aggregate_type
        self._event_store = event_store
        self._factory = factory
        self._apply = apply

    async def get(self, aggregate_id: IdT) -> Aggregate[IdT]:
        aggregate = self._factory(aggregate_id)
        events = await self._event_store.load(self._aggregate_type, aggregate_id)
        for event in events:
            self._apply(aggregate, event)
        return aggregate

    async def save(self, aggregate: Aggregate[IdT]) -> None:
        # If events are not yet published, publish them; otherwise reuse
        events: list["DomainEvent[Any, Any]"]
        if any(e.published_at is None for e in aggregate.events):
            events = aggregate.publish_events()
        else:
            events = list(aggregate.events)
        try:
            await self._event_store.append(events)
        finally:
            aggregate.clear_events()
