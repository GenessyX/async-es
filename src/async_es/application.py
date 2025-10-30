from __future__ import annotations

from typing import TYPE_CHECKING

from async_es.types import AggregateId

if TYPE_CHECKING:
    from async_es.aggregate import Aggregate
    from async_es.protocols import ApplicationNotifier, Repository


class Application[IdT: AggregateId]:
    def __init__(self, repository: "Repository[IdT]", notifier: "ApplicationNotifier | None" = None) -> None:
        self._repository = repository
        self._notifier = notifier

    async def load(self, aggregate_id: IdT) -> "Aggregate[IdT]":
        return await self._repository.get(aggregate_id)

    async def save(self, aggregate: "Aggregate[IdT]") -> None:
        # publish events so notifier sees published timestamps
        events = aggregate.publish_events()
        await self._repository.save(aggregate)
        if self._notifier and events:
            await self._notifier.notify(events)
