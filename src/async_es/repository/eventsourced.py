from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

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
        aggregate_cls: type["Aggregate[IdT]"],
        apply: "Callable[[Aggregate[IdT], DomainEvent[IdT, Any]], None] | None" = None,
    ) -> None:
        self._aggregate_type = aggregate_type
        self._event_store = event_store
        self._aggregate_cls = aggregate_cls
        self._apply = apply

    async def get(self, aggregate_id: IdT) -> Aggregate[IdT]:
        aggregate = self._aggregate_cls(id=aggregate_id)
        events = await self._event_store.load(self._aggregate_type, aggregate_id)
        if self._apply is not None:
            # drop any construction events if stream exists
            if events:
                aggregate.clear_events()
            for event in events:
                self._apply(aggregate, event)
            return aggregate

        # Method-based replay using original event-producing methods, suppressing new events
        if events:
            aggregate.clear_events()
        with aggregate.suppress_events():
            for event in events:
                self._replay_with_methods(aggregate, event)
        if events:
            first = events[0]
            last = events[-1]
            if hasattr(aggregate, "created_at"):
                aggregate.created_at = first.occurred_at
            if hasattr(aggregate, "updated_at"):
                aggregate.updated_at = last.occurred_at
        return aggregate

    def _replay_with_methods(self, aggregate: "Aggregate[IdT]", event: "DomainEvent[IdT, Any]") -> None:
        payload = event.payload
        # Find method decorated with matching event_type
        for name in dir(aggregate):
            attr = getattr(aggregate, name)
            if callable(attr) and getattr(attr, "__event_type__", None) == event.event_type:
                # Call with payload as args/kwargs
                if hasattr(payload, "__dict__"):
                    try:
                        data = cast("dict[str, Any]", payload.__dict__)
                        attr(**data)
                    except TypeError:
                        attr(payload)
                else:
                    attr(payload)
                return
        # If no method found, ignore silently

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
